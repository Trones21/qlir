from __future__ import annotations
import logging
import os
import random

from httpx import HTTPStatusError, RequestError

from qlir.data.sources.binance.endpoints.klines.fetch import FetchFailed
from qlir.data.sources.binance.endpoints.klines.manifest.rebuild.from_responses import rebuild_manifest_from_responses
from qlir.data.sources.binance.endpoints.klines.manifest.summary import update_summary
from qlir.data.sources.binance.manifest_delta_log import append_delta_log_to_in_memory_manifest, append_manifest_delta
from qlir.data.sources.common import claims
from qlir.data.sources.binance.endpoints.klines.manifest.manifest import MANIFEST_FILENAME, load_or_create_manifest, write_full_manifest_snapshot, seed_manifest_with_expected_slices, update_manifest_with_classification
from qlir.data.sources.common.slices.slice_classification import classify_slices
from qlir.data.sources.common.slices.slice_key import SliceKey, get_current_slice_key
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_policy import SliceStatusPolicy
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.io.helpers import has_files
from qlir.telemetry import telemetry
from qlir.time.iso import now_utc, parse_iso
from qlir.utils.str.color import Ansi, colorize
from qlir.utils.time.fmt import format_ts_human
log = logging.getLogger(__name__)
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from .model import REQUIRED_FIELDS #SliceStatus, classify_slices
from .urls import generate_kline_slices
from .fetch_wrapper import log_requested_slice_size, fetch_and_persist_slice
from .time_range import compute_time_range
from qlir.data.sources.binance.endpoints.klines.manifest.validation.orchestrator import validate_manifest_and_fs_integrity
from qlir.data.core.paths import get_data_root, get_symbol_interval_limit_raw_dir

IN_PROGRESS_STALE_SEC = 300  # 5 minutes

def is_stale(entry):
    if entry["slice_status"] != "in_progress":
        return False
    ts = entry.get("requested_at")
    if not ts:
        return True
    return (now_utc() - parse_iso(ts)).total_seconds() > IN_PROGRESS_STALE_SEC


def run_klines_worker(
    data_root: Path,
    symbol: str,
    interval: str,
    limit: int = 1000,
    poll_interval_sec: float = 10.0,
    max_backoff_sec: float = 60.0,
) -> None:
    """
    Main completeness loop for Binance /api/v3/klines.

    This worker will:

        1. Resolve the on-disk root for this symbol+interval.
        2. In a loop:
            a. Determine the time range to cover (start_ms -> now_ms).
            b. Enumerate all expected slices (KlineSliceKey).
            c. Load manifest.json and determine missing/failed/partial/needs_refresh(broken meta contract) slices.
            d. Fetch + persist each slice in to_fetch.
            e. Update manifest.json and summary stats.
            f. Sleep briefly, then repeat.

    Args:
        symbol:
            Trading pair symbol, e.g. "BTCUSDT".

        interval:
            Kline interval, currently "1s" or "1m" in our design.

        limit:
            Max candles per request. For this module, we assume 1000
            and design slicing accordingly.

        data_root:
            Optional explicit data root. If None, we use get_data_root(user_root)
            which resolves:
                1. user_root param passed
                2. QLIR_DATA_ROOT env var
                3. default ~/qlir_data

        poll_interval_sec:
            How long to sleep when there is nothing missing to fetch
            (between iterations).

        max_backoff_sec:
            Upper bound for exponential backoff on repeated failures.
    """
    # Resolve data root and this symbol+interval directory
    sym_interval_limit_raw_dir = get_symbol_interval_limit_raw_dir(
        data_root=data_root,
        datasource="binance", 
        endpoint="klines",
        symbol=symbol,
        interval=interval,
        limit=limit
    )

    # Get Paths 
    responses_dir = sym_interval_limit_raw_dir.joinpath("responses")
    _ensure_dir(sym_interval_limit_raw_dir)
    _ensure_dir(responses_dir)
    log.info("Saving raw reponses to: %s", responses_dir)

    claims_dir = sym_interval_limit_raw_dir  # base_dir passed to claims.py
    log.debug("Locks written to: %s", claims_dir)

    delta_log_path = responses_dir.parent / "manifest.delta"
    log.debug("Manifest delta log written to: %s", delta_log_path)

    manifest_path = sym_interval_limit_raw_dir.joinpath(MANIFEST_FILENAME)
    log.debug("and the manifest aggregator batch updates manifest located at: %s", manifest_path)

    snapshot_dir = sym_interval_limit_raw_dir.joinpath("manifest_snapshots")

    if os.getenv("QLIR_MANIFEST_LOG"):
        log.debug("manifest batch update worker logs are turned on. To view, open another terminal and use tail -f %s", manifest_path)

    backoff = 1.0

    while True:
        # We need the current slice b/c we release this lock and then end of the while loop
        # so that we can reacquire it in the for loop (otherwise it stays locked from previous runs) 
        current_slice_id: str | None = None

        min_start_ms, max_end_ms = compute_time_range(symbol=symbol, interval=interval, limit=1000)

        expected_slices = _enumerate_expected_slices(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_ms=min_start_ms,
            end_ms=max_end_ms,
        )

        manifest = load_or_create_manifest(
            manifest_path=manifest_path,
            symbol=symbol,
            interval=interval,
            limit=limit,
        )

        append_delta_log_to_in_memory_manifest(delta_log_path=delta_log_path, manifest=manifest)

        validate_manifest_and_fs_integrity(manifest, responses_dir)
        log.info(f"Total Expected Slice Count:{len(expected_slices)}")

        log.warning("Full writes in terribly inefficient, but eventually the manifest validation will be pulled out and only run like once per hour")
  
        if manifest["slices"] == {} and has_files(responses_dir):
            log.info(f"Manifest contains empty slices dict, but responses dir has response files. Rebuilding from filesystem raw responses dir: {responses_dir} ")
            manifest = rebuild_manifest_from_responses(responses_dir=responses_dir, 
                                                       expected_slices=expected_slices, 
                                                       symbol=symbol,
                                                       interval=interval,
                                                       limit=limit)
            write_full_manifest_snapshot(snapshot_dir=snapshot_dir, manifest=manifest, reason=f"Manifest contains empty slices dict, but responses dir has response files. Rebuilding from filesystem raw responses dir: {responses_dir} ")
        
        
        # Implement later 
        # if report.fs_violation_ratio > cfg.rebuild_threshold:
        #     log.warning("Integrity violations exceed threshold; rebuilding manifest")
        #     manifest = rebuild_manifest_from_responses(...)


        if seed_manifest_with_expected_slices(manifest, expected_slices):
            write_full_manifest_snapshot(snapshot_dir=snapshot_dir, manifest=manifest, reason=f"Writing entire manifest snapshot - Updating Manifest with range {format_ts_human(min_start_ms)} , {format_ts_human(max_end_ms)}")
        
        classified = classify_slices(expected_slices, manifest)
        manifest = update_manifest_with_classification(manifest=manifest, classified=classified)
        write_full_manifest_snapshot(snapshot_dir=snapshot_dir, manifest=manifest, reason=f"Writing entire manifest snapshot - Updating Manifest with slice classifications")
        
        # This is where we release the lock for the current slice
        prior_key = next(reversed(manifest["slices"]))
        current_comp_key = get_current_slice_key(prior_key)
        entry = manifest["slices"].get(current_comp_key)
        if entry:
            slice_id = entry["slice_id"]
            try:
                claims.release_claim(claims_dir, slice_id)
                log.debug("Released previous current slice claim at iteration start")
            except FileNotFoundError:
                pass


        to_fetch = _construct_fetch_batch(classified)

        # No work to do; reset backoff and sleep for a bit.
        if not to_fetch:
            active = claims.list_claims(claims_dir)
            log.info(
                "No work to do | active_claims=%d",
                len(active),
            )
            backoff = 1.0
            time.sleep(poll_interval_sec)
            print('\n')
            continue
        
        fetch_comp_keys = [skey.canonical_slice_composite_key() for skey in to_fetch]
        for slice_key in to_fetch:
            log.info(f"{slice_key}, start (utc-0):{format_ts_human(slice_key.start_ms)}, end(utc-0): {format_ts_human(slice_key.end_ms)}")
            
            fetch_fail: FetchFailed | None = None
            meta: Dict[str, Any] = {}
            slice_comp_key = slice_key.canonical_slice_composite_key()

            print('\n')
            if slice_comp_key not in fetch_comp_keys:
                log.debug(f'fetching {slice_comp_key}, but its not in to_fetch {fetch_comp_keys}')
            
            entry = manifest["slices"][slice_comp_key]
            slice_id = entry["slice_id"]

            # ---- CLAIM GATE (replaces IN_PROGRESS logic) ----

            if claims.is_claimed(claims_dir, slice_id):
                if not claims.reclaim_if_stale(
                    claims_dir,
                    slice_id,
                    ttl_sec=IN_PROGRESS_STALE_SEC,
                ):
                    log.debug("Slice is claimed - continuing to next slice")
                    continue

            if not claims.try_claim(
                claims_dir,
                slice_id,
                payload={
                    "slice_comp_key": slice_comp_key,
                    "symbol": symbol,
                    "interval": interval,
                },
            ):
                continue

            # ---- OWNERSHIP ACQUIRED ----

            try:
                entry.setdefault("request_count", {}).setdefault("fetches", 0)
                entry["request_count"]["fetches"] += 1

                fetch_result = fetch_and_persist_slice(
                    request_slice_key=slice_key,
                    data_root=data_root,
                    responses_dir=responses_dir,
                )

                if fetch_result is FetchFailed:
                    fetch_fail = fetch_result
                    raise fetch_fail
                else:
                    # this whole entire fetch, wrapper and stuff prob needs to be refactored... but it works and its not that bad...
                    meta = fetch_result #type: ignore

                entry["requested_at"] = now_utc().isoformat()
                entry = _update_entry(meta, entry)
                manifest["slices"][slice_comp_key] = entry

                update_summary(manifest)

                append_manifest_delta(
                    delta_log_path=delta_log_path,
                    delta=entry
                )

                backoff = 1.0

            except Exception as exc:
                slice_status_reason = _get_slice_status_reason_on_exception(exc, fetch_fail)

                entry = _update_entry_on_exception(
                    entry=entry,
                    meta=meta,
                    slice_status_reason=slice_status_reason,
                    exception=exc,
                )

                manifest["slices"][slice_comp_key] = entry
                update_summary(manifest)

                failure_delta = {
                    "slice_comp_key": slice_comp_key,
                    "slice_id": slice_id,
                    "slice_status": entry["slice_status"],
                    "slice_status_reason": entry["slice_status_reason"],
                    "error": str(exc),
                    "completed_at": now_utc().isoformat(),
                }
                append_manifest_delta(
                    delta_log_path=delta_log_path,
                    delta=failure_delta,
                )

                log.exception(exc)
                time.sleep(backoff)
                backoff = _next_backoff(current=backoff, cap=max_backoff_sec)

            finally:
                # 🔑 ALWAYS release the claim
                claims.release_claim(claims_dir, slice_id)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _next_backoff(current, *, base=1.0, cap=60.0):
    next_val = min(current * 2.0, cap)
    jitter = random.uniform(0.8, 1.2)
    return max(base, next_val * jitter)



def _failure_msg(entry) -> str:
    status = entry.get("http_status")
    reason = entry.get("slice_status_reason")
    slice_status = colorize(entry["slice_status"], Ansi.RED, Ansi.BOLD)

    if status is None:
        return f"Request failed (no HTTP response, reason={reason}) - marking slice as {slice_status}"
    else:
        return f"Request failed (http_status={status}, reason={reason}) - marking slice as {slice_status}"



def _construct_fetch_batch(classified):
        
        by_classification = {
        "missing": len(classified.missing),
        "partial": len(classified.partial),
        "needs_refresh": len(classified.needs_refresh),
        "failed": len(classified.failed),
        }

        to_fetch = [
            *classified.needs_refresh,
            *classified.missing,
            *classified.failed,
            *classified.partial,            
        ]

        log.info(f"{len(to_fetch)} slices to fetch")
        log.debug(
        "Fetch batch constructed",
            extra={
                "count": len(to_fetch),
                "by_classification": by_classification,
                "to_fetch": to_fetch,
            }
        )

        return to_fetch

def _get_slice_status_reason_on_exception(exc, fetch_fail):
        if fetch_fail is not None:
            return SliceStatusReason.NETWORK_UNAVAILABLE

        if exc is HTTPStatusError:
            return SliceStatusReason.HTTP_ERROR
   
        return SliceStatusReason.EXCEPTION

def _update_entry_on_exception(entry, meta, slice_status_reason: SliceStatusReason, exception):
        
        now_iso = _now_iso()

        entry.update(
            {
                "slice_status": SliceStatusPolicy.from_failure_reason(slice_status_reason),
                "slice_status_reason": slice_status_reason,
                "http_status": (
                    meta.get("http_status")
                    if meta is not None
                    else "exception_occured_before_fetch_and_persist_slice_func_returned"
                ),
                "error": str(exception),
                "occurred_at": now_iso,
            }
        )
        entry = _add_or_update_entry_meta_contract(entry, expected_fields=REQUIRED_FIELDS)
        return entry


def _update_entry(meta, entry) -> Dict:

        entry.update(
            {
                "slice_id": meta["slice_id"],
                "slice_comp_key": meta["slice_comp_key"],
                "relative_path": meta["relative_path"],
                "slice_status": meta['slice_status'],
                "slice_status_reason":meta['slice_status_reason'],
                "http_status": meta.get("http_status"),
                "n_items": meta.get("n_items"),
                "first_ts": meta.get("first_ts"),
                "last_ts": meta.get("last_ts"),
                "requested_url": meta.get("url"),
                "requested_at": meta.get("requested_at"),
                "completed_at": meta.get("completed_at"),
                "error": None,
            }
        )
        entry_keys = set(entry.keys()) - {"__meta_contract"}
        if set(meta.keys()) != entry_keys:
            log.debug(f"[Fyi] - Not all keys returned as meta from fetch_and_persist_slice are written to disk. \n Entry Keys: {entry_keys} \n Meta Keys: {meta.keys()}")
        
        entry = _add_or_update_entry_meta_contract(entry=entry, expected_fields=REQUIRED_FIELDS)
        return entry



def inspect_klines(raw: list[list], interval_ms: int) -> dict:
    if not raw:
        return {"n": 0}

    opens = [int(r[0]) for r in raw]  # open_time ms
    opens_sorted = sorted(opens)
    uniq = len(set(opens_sorted))

    deltas = [opens_sorted[i+1] - opens_sorted[i] for i in range(len(opens_sorted)-1)]
    gaps = [d for d in deltas if d != interval_ms]

    return {
        "n": len(raw),
        "uniq_open": uniq,
        "first_open": opens_sorted[0],
        "last_open": opens_sorted[-1],
        "min_delta": min(deltas) if deltas else None,
        "max_delta": max(deltas) if deltas else None,
        "n_gaps": len(gaps),
        "max_gap_ms": max(gaps) if gaps else 0,
    }


def _add_or_update_entry_meta_contract(entry, expected_fields):
    present = set(entry.keys())
    missing = [f for f in expected_fields if f not in present]

    if missing:
        entry["__meta_contract"] = {
            "status": "out_of_sync",
            "missing_fields": missing,
        }
    else:
        entry["__meta_contract"] = {
        "status": "ok",        
        "missing_fields": [],
        "notes": None,
        }

    return entry


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enumerate_expected_slices(
    symbol: str,
    interval: str,
    limit: int,
    start_ms: int,
    end_ms: int,
) -> List[SliceKey]:
    """
    Convert the time range into a list of KlineSliceKey objects.
    """
    slices: List[SliceKey] = []
    for s, e in generate_kline_slices(symbol, interval, start_ms, end_ms, limit):
        slices.append(
            SliceKey(
                symbol=symbol,
                interval=interval,
                start_ms=s,
                end_ms=e,
                limit=limit,
            )
        )
    log.debug(f"_enumerate_epected_slices. Last slice in array: {format_ts_human(slices[-1].start_ms)} - {format_ts_human(slices[-1].end_ms)}")
    return slices
