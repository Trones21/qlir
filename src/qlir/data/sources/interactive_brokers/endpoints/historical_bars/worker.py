from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
import random
import time
from typing import Any, Dict, List

from qlir.data.core.paths import get_symbol_interval_limit_raw_dir
from qlir.data.sources.common import claims
from qlir.data.sources.common.slices.slice_classification import classify_slices
from qlir.data.sources.common.slices.slice_key import SliceKey, get_current_slice_key
from qlir.data.sources.common.slices.slice_status_policy import SliceStatusPolicy
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.data.sources.interactive_brokers import bar_sizes
from qlir.data.sources.interactive_brokers.preflight import run_preflight
from qlir.data.sources.interactive_brokers.manifest_delta_log import (
    append_delta_log_to_in_memory_manifest,
    append_manifest_delta,
)
from qlir.data.sources.interactive_brokers.slices import generate_bar_slices
from qlir.io.delete import delete_file_if_exists
from qlir.io.helpers import has_files
from qlir.time.iso import now_utc, parse_iso

from .client import IBKRHistoricalClient
from .fetch import FetchFailed
from .fetch_wrapper import fetch_and_persist_slice
from .manifest.manifest import (
    MANIFEST_FILENAME,
    load_or_create_manifest,
    seed_manifest_with_expected_slices,
    update_manifest_with_classification,
    wait_for_load_manifest,
    write_full_manifest_snapshot,
)
from .manifest.rebuild.from_responses import rebuild_manifest_from_responses
from .manifest.summary import update_summary
from .manifest.validation.orchestrator import validate_manifest_and_fs_integrity
from .model import REQUIRED_FIELDS

log = logging.getLogger(__name__)

IN_PROGRESS_STALE_SEC = 300  # 5 minutes
DEFAULT_MIN_REQUEST_INTERVAL_SEC = 10.0  # IBKR historical pacing (~<=60 requests / 10 min)


def run_historical_bars_worker(
    data_root: Path,
    symbol: str,
    interval: str,
    limit: int = 1000,
    *,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    primary_exchange: str | None = None,
    what_to_show: str = "TRADES",
    use_rth: int = 0,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 1,
    poll_interval_sec: float = 10.0,
    max_backoff_sec: float = 60.0,
    min_request_interval_sec: float = DEFAULT_MIN_REQUEST_INTERVAL_SEC,
) -> None:
    """
    Completeness loop for IBKR historical bars (the Fetcher process).

    Mirrors the Binance klines worker; the differences are: a persistent IBKR client
    (created once), IBKR pacing between requests, and equity-aware slice inspection.
    """
    canonical_interval = bar_sizes.normalize_interval(interval)
    interval_ms = bar_sizes.interval_to_ms(canonical_interval)
    bar_size_setting = bar_sizes.ibkr_bar_size(canonical_interval)

    sym_interval_limit_raw_dir = get_symbol_interval_limit_raw_dir(
        data_root=data_root,
        datasource="interactive_brokers",
        endpoint="historical_bars",
        symbol=symbol,
        interval=canonical_interval,
        limit=limit,
    )

    responses_dir = sym_interval_limit_raw_dir.joinpath("responses")
    _ensure_dir(sym_interval_limit_raw_dir)
    _ensure_dir(responses_dir)
    log.info("Saving raw responses to: %s", responses_dir)

    claims_dir = sym_interval_limit_raw_dir
    delta_log_path = sym_interval_limit_raw_dir / "manifest.delta"
    manifest_path = sym_interval_limit_raw_dir.joinpath(MANIFEST_FILENAME)
    snapshot_dir = sym_interval_limit_raw_dir.joinpath("manifest_snapshot")
    snapshot_path = snapshot_dir / "manifest.snapshot.json"

    log.info("Removing manifest.json/.delta/.snapshot — manifest MUST be rebuilt on startup")
    delete_file_if_exists(manifest_path)
    delete_file_if_exists(delta_log_path)
    delete_file_if_exists(snapshot_path)

    # ---- preflight: fail loud + actionable if the Gateway isn't reachable ----
    if not run_preflight(host=host, port=port):
        raise RuntimeError(
            f"IBKR preflight failed — IB Gateway/TWS not reachable at {host}:{port}. "
            "See the preflight report logged above."
        )

    # ---- one persistent IBKR client for the whole worker lifetime ----
    client = IBKRHistoricalClient.connect(
        host=host,
        port=port,
        client_id=client_id,
        symbol=symbol,
        sec_type=sec_type,
        exchange=exchange,
        currency=currency,
        primary_exchange=primary_exchange,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )

    from .time_range import compute_time_range  # local import: pulls ib_async via client only

    backoff = 1.0
    last_request_monotonic = 0.0

    try:
        while True:
            min_start_ms, max_end_ms = compute_time_range(client)

            expected_slices = _enumerate_expected_slices(
                symbol=symbol,
                interval=canonical_interval,
                limit=limit,
                interval_ms=interval_ms,
                start_ms=min_start_ms,
                end_ms=max_end_ms,
            )

            manifest = load_or_create_manifest(
                manifest_path=manifest_path,
                symbol=symbol,
                interval=canonical_interval,
                limit=limit,
            )

            if manifest["slices"] == {} and has_files(responses_dir):
                log.info("Empty manifest but responses exist — rebuilding from filesystem")
                manifest_snap = rebuild_manifest_from_responses(
                    responses_dir=responses_dir,
                    expected_slices=expected_slices,
                    symbol=symbol,
                    interval=canonical_interval,
                    limit=limit,
                )
                write_full_manifest_snapshot(
                    snapshot_dir=snapshot_dir,
                    manifest=manifest_snap,
                    reason="Rebuilt manifest from filesystem responses",
                )
                manifest = wait_for_load_manifest(manifest_path)
            else:
                log.info("Manifest slices_len: %d", len(manifest["slices"]))

            append_delta_log_to_in_memory_manifest(delta_log_path=delta_log_path, manifest=manifest)
            validate_manifest_and_fs_integrity(manifest, responses_dir)
            log.info("Total expected slice count: %d", len(expected_slices))

            if seed_manifest_with_expected_slices(manifest, expected_slices):
                write_full_manifest_snapshot(
                    snapshot_dir=snapshot_dir,
                    manifest=manifest,
                    reason="Seeding manifest with expected slices",
                )

            classified = classify_slices(expected_slices, manifest)
            manifest = update_manifest_with_classification(manifest=manifest, classified=classified)
            write_full_manifest_snapshot(
                snapshot_dir=snapshot_dir,
                manifest=manifest,
                reason="Updating manifest with slice classifications",
            )

            # Release the current (open) slice claim so it can be re-acquired below.
            if manifest["slices"]:
                prior_key = next(reversed(manifest["slices"]))
                current_comp_key = get_current_slice_key(prior_key)
                entry = manifest["slices"].get(current_comp_key)
                if entry:
                    try:
                        claims.release_claim(claims_dir, entry["slice_id"])
                    except FileNotFoundError:
                        pass

            to_fetch = _construct_fetch_batch(classified)

            if not to_fetch:
                log.info("No work to do | active_claims=%d", len(claims.list_claims(claims_dir)))
                backoff = 1.0
                time.sleep(poll_interval_sec)
                continue

            for slice_key in to_fetch:
                fetch_fail: FetchFailed | None = None
                meta: Dict[str, Any] = {}
                slice_comp_key = slice_key.canonical_slice_composite_key()
                entry = manifest["slices"][slice_comp_key]
                slice_id = entry["slice_id"]

                # ---- CLAIM GATE ----
                if claims.is_claimed(claims_dir, slice_id):
                    if not claims.reclaim_if_stale(claims_dir, slice_id, ttl_sec=IN_PROGRESS_STALE_SEC):
                        continue
                if not claims.try_claim(
                    claims_dir,
                    slice_id,
                    payload={"slice_comp_key": slice_comp_key, "symbol": symbol, "interval": canonical_interval},
                ):
                    continue

                # ---- OWNERSHIP ACQUIRED ----
                try:
                    # IBKR pacing: keep >= min_request_interval_sec between historical requests.
                    last_request_monotonic = _respect_pacing(
                        last_request_monotonic, min_request_interval_sec
                    )

                    entry.setdefault("request_count", {}).setdefault("fetches", 0)
                    entry["request_count"]["fetches"] += 1

                    fetch_result = fetch_and_persist_slice(
                        client=client,
                        request_slice_key=slice_key,
                        interval_ms=interval_ms,
                        bar_size_setting=bar_size_setting,
                        data_root=data_root,
                        responses_dir=responses_dir,
                    )

                    if isinstance(fetch_result, FetchFailed):
                        fetch_fail = fetch_result
                        raise fetch_fail

                    meta = fetch_result  # type: ignore[assignment]

                    entry["requested_at"] = now_utc().isoformat()
                    entry = _update_entry(meta, entry)
                    manifest["slices"][slice_comp_key] = entry
                    update_summary(manifest)
                    append_manifest_delta(delta_log_path=delta_log_path, delta=entry)
                    backoff = 1.0

                except Exception as exc:
                    slice_status_reason = _get_slice_status_reason_on_exception(exc, fetch_fail)
                    entry = _update_entry_on_exception(
                        entry=entry, meta=meta, slice_status_reason=slice_status_reason, exception=exc
                    )
                    manifest["slices"][slice_comp_key] = entry
                    update_summary(manifest)
                    append_manifest_delta(
                        delta_log_path=delta_log_path,
                        delta={
                            "slice_comp_key": slice_comp_key,
                            "slice_id": slice_id,
                            "slice_status": entry["slice_status"],
                            "slice_status_reason": entry["slice_status_reason"],
                            "error": str(exc),
                            "completed_at": now_utc().isoformat(),
                        },
                    )
                    log.exception(exc)
                    time.sleep(backoff)
                    backoff = _next_backoff(current=backoff, cap=max_backoff_sec)

                finally:
                    claims.release_claim(claims_dir, slice_id)
    finally:
        client.disconnect()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _respect_pacing(last_request_monotonic: float, min_interval_sec: float) -> float:
    now = time.monotonic()
    wait = min_interval_sec - (now - last_request_monotonic)
    if wait > 0:
        log.debug("Pacing: sleeping %.2fs before next IBKR historical request", wait)
        time.sleep(wait)
    return time.monotonic()


def _next_backoff(current, *, base=1.0, cap=60.0):
    next_val = min(current * 2.0, cap)
    jitter = random.uniform(0.8, 1.2)
    return max(base, next_val * jitter)


def _construct_fetch_batch(classified):
    to_fetch = [
        *classified.needs_refresh,
        *classified.missing,
        *classified.failed,
        *classified.partial,
    ]
    log.info("%d slices to fetch", len(to_fetch))
    return to_fetch


def _get_slice_status_reason_on_exception(exc, fetch_fail):
    if fetch_fail is not None:
        return fetch_fail.reason
    return SliceStatusReason.EXCEPTION


def _update_entry_on_exception(entry, meta, slice_status_reason: SliceStatusReason, exception):
    entry.update(
        {
            "slice_status": SliceStatusPolicy.from_failure_reason(slice_status_reason),
            "slice_status_reason": slice_status_reason,
            "http_status": None,
            "error": str(exception),
            "occurred_at": _now_iso(),
        }
    )
    return _add_or_update_entry_meta_contract(entry, expected_fields=REQUIRED_FIELDS)


def _update_entry(meta, entry) -> Dict:
    entry.update(
        {
            "slice_id": meta["slice_id"],
            "slice_comp_key": meta["slice_comp_key"],
            "relative_path": meta["relative_path"],
            "slice_status": meta["slice_status"],
            "slice_status_reason": meta["slice_status_reason"],
            "http_status": meta.get("http_status"),
            "n_items": meta.get("n_items"),
            "first_ts": meta.get("first_ts"),
            "last_ts": meta.get("last_ts"),
            "received_first_open": meta.get("received_first_open"),
            "received_last_open": meta.get("received_last_open"),
            "requested_at": meta.get("requested_at"),
            "completed_at": meta.get("completed_at"),
            "error": None,
        }
    )
    return _add_or_update_entry_meta_contract(entry=entry, expected_fields=REQUIRED_FIELDS)


def _add_or_update_entry_meta_contract(entry, expected_fields):
    present = set(entry.keys())
    missing = [f for f in expected_fields if f not in present]
    if missing:
        entry["__meta_contract"] = {"status": "out_of_sync", "missing_fields": missing}
    else:
        entry["__meta_contract"] = {"status": "ok", "missing_fields": [], "notes": None}
    return entry


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enumerate_expected_slices(
    symbol: str,
    interval: str,
    limit: int,
    interval_ms: int,
    start_ms: int,
    end_ms: int,
) -> List[SliceKey]:
    slices: List[SliceKey] = []
    for s, e in generate_bar_slices(interval_ms, start_ms, end_ms, limit):
        slices.append(SliceKey(symbol=symbol, interval=interval, start_ms=s, end_ms=e, limit=limit))
    return slices
