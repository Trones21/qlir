from __future__ import annotations
import logging
import random

from httpx import HTTPStatusError, RequestError

from qlir.data.sources.binance.endpoints.klines.manifest.manifest import MANIFEST_FILENAME, load_or_create_manifest, save_manifest, seed_manifest_with_expected_slices, update_manifest_with_classification
from qlir.data.sources.common.slices.entry_serializer import serialize_entry
from qlir.data.sources.common.slices.manifest_serializer import serialize_manifest
from qlir.data.sources.common.slices.slice_classification import classify_slices
from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_policy import SliceStatusPolicy
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.time.iso import now_utc, parse_iso
from qlir.utils.enum import enum_for_log, serialize_enum
from qlir.utils.str.color import Ansi, colorize
from qlir.utils.time.fmt import format_ts_human
log = logging.getLogger(__name__)
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from .model import REQUIRED_FIELDS #SliceStatus, classify_slices
from .urls import generate_kline_slices
from .fetch import log_requested_slice_size, fetch_and_persist_slice
from .time_range import compute_time_range
from qlir.data.sources.binance.endpoints.klines.manifest.validation.orchestrator import validate_manifest_and_fs_integrity
from qlir.data.core.paths import get_data_root

IN_PROGRESS_STALE_SEC = 300  # 5 minutes

def is_stale(entry):
    if entry["slice_status"] != "in_progress":
        return False
    ts = entry.get("requested_at")
    if not ts:
        return True
    return (now_utc() - parse_iso(ts)).total_seconds() > IN_PROGRESS_STALE_SEC


def run_klines_worker(
    symbol: str,
    interval: str,
    limit: int = 1000,
    data_root: Optional[Path | str] = None,
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
    root = get_data_root(data_root)
    sym_interval_limit_dir = (
        Path(root)
        .joinpath("binance", "klines", "raw", symbol, interval, f"limit={limit}")
        .resolve()
    )
    responses_dir = sym_interval_limit_dir.joinpath("responses")
    manifest_path = sym_interval_limit_dir.joinpath(MANIFEST_FILENAME)

    _ensure_dir(sym_interval_limit_dir)
    _ensure_dir(responses_dir)
    log.info("Saving raw reponses to: %s", responses_dir)
    backoff = 1.0

    while True:
        # TODO: Update _compute_time_range 
        # This is where any off by 1 ms will occur ... because end_ms is not last open (with current logic)
        # Need to give a param to compute time range so that it returns first_open and last_open (the logic seems to be there but its commented out)
        # this would be a good one to write a unit test for       
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
        validate_manifest_and_fs_integrity(manifest, responses_dir)
        log.info(f"Total Expected Slice Count:{len(expected_slices)}")
    
        if seed_manifest_with_expected_slices(manifest, expected_slices):
            save_manifest(manifest_path, manifest, f"Updating Manifest with range {format_ts_human(min_start_ms)} , {format_ts_human(max_end_ms)}")  # persist full universe (for external visualization/stats - no need to see slices in memory, b/c they are all in the manifest.json)
        
        
        classified = classify_slices(expected_slices, manifest)
        manifest = update_manifest_with_classification(manifest=manifest, classified=classified)
        save_manifest(manifest_path, manifest, f"Updating Manifest with slice classifications" )

        to_fetch = _construct_fetch_batch(classified)

        # No work to do; reset backoff and sleep for a bit.
        if not to_fetch:
            backoff = 1.0
            log.info(f"No missing or partial slices, sleeping for {poll_interval_sec} seconds")
            time.sleep(poll_interval_sec)
            print('\n')
            continue
        
        fetch_comp_keys = [skey.canonical_slice_composite_key() for skey in to_fetch]
        for slice_key in to_fetch:
            meta: dict | None = None
            fetch_fail = None
            slice_comp_key = slice_key.canonical_slice_composite_key()
            print('\n')
            if slice_comp_key not in fetch_comp_keys:
                log.debug(f'fetching {slice_comp_key}, but its not in to_fetch {fetch_comp_keys}')
            
            try:
                entry = manifest["slices"].get(slice_comp_key)
            except Exception as exc:
                raise KeyError(f"Unable to find a manifest entry for key: {slice_comp_key}. You have a corrupt manifest or the server passed an incorrect slice key (code error)")
            try:
                # Skip if another proc is currently handling this 
                # (with stale safeguard to prevent items from being locked forever, in case the proc is killed while fetch is being performed)
                if entry.get("slice_status") == SliceStatus.IN_PROGRESS and not is_stale(entry):
                    continue
                log.info(
                    f"Slice entry pulled from manifest | {slice_comp_key} | "
                    f"id={entry['slice_id']} status={entry.get('slice_status')}"
                )
                log.debug(f"""{colorize('=== Slice entry pulled from manifest ===', Ansi.BOLD)}
comp_key: {colorize(slice_comp_key, Ansi.BOLD)}
hashed: {colorize(entry['slice_id'], Ansi.BOLD)}
Full Entry: {entry}""")
                          
                # Mark in-progress
                entry["slice_status"] = SliceStatus.IN_PROGRESS     
                save_manifest(manifest_path, manifest, f"temporarily marking slice as in progress (while fetch occurs)")

                # Increment fetch counter for *any* actual fetch
                entry.setdefault("request_count", {}).setdefault("fetches", 0)
                entry["request_count"]["fetches"] += 1

                meta, fetch_fail = fetch_and_persist_slice(
                    request_slice_key=slice_key,
                    data_root=root,
                    responses_dir=responses_dir,
                )

                if fetch_fail is not None:
                    raise fetch_fail

                entry["requested_at"] = now_utc().isoformat()
                entry = _update_entry(meta, entry)
                manifest["slices"][slice_comp_key] = entry

                _update_summary(manifest)
                save_manifest(manifest_path, manifest, f"fetch slice succeeded - marking as {colorize(enum_for_log(entry['slice_status']), Ansi.GREEN , Ansi.BOLD)}. Slice Status Reason: {enum_for_log(entry['slice_status_reason'])}")

                # Reset backoff on success
                backoff = 1.0

            # Mark slice as failed and backoff
            except Exception as exc:
                
                slice_status_reason = _get_slice_status_reason_on_exception(exc, fetch_fail)
                entry = _update_entry_on_exception(entry=entry, meta=meta, slice_status_reason=slice_status_reason, exception=exc)
                
                # Update and Write Manifest
                manifest["slices"][slice_comp_key] = entry
                _update_summary(manifest)

                save_manifest(
                    manifest_path,
                    serialize_manifest(manifest),
                    _failure_msg(entry),
                )

                log.exception(exc)
                time.sleep(backoff)
                backoff = _next_backoff(current=backoff, cap=max_backoff_sec)


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
            *classified.missing,
            *classified.partial,
            *classified.needs_refresh,
            *classified.failed
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

def _get_slice_status_reason_on_exception(exc, fetch_failed):
        if fetch_failed is not None:
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
    return slices

def _update_summary(manifest: Dict) -> None:
    """
    Recompute summary stats based on current slice entries.
    """
    slices = manifest.get("slices", {})
    total = len(slices)
    complete = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.COMPLETE)
    partial = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.PARTIAL)
    failed = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.FAILED)
    missing = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.MISSING)
    needs_refresh = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.NEEDS_REFRESH)
    in_progress = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.IN_PROGRESS)

    accounted_for = (
        complete
        + partial
        + failed
        + missing
        + needs_refresh
        + in_progress
    )

    if accounted_for != total:
        log.warning(
            "Manifest summary mismatch: total=%d accounted=%d "
            "(complete=%d partial=%d missing=%d needs_refresh=%d "
            "in_progress=%d failed=%d)",
            total,
            accounted_for,
            complete,
            partial,
            missing,
            needs_refresh,
            in_progress,
            failed,
        )

    manifest["summary"] = {
        "total_slices": total,
        "missing_slices": missing,
        "complete_slices": complete,
        "partial_slices": partial,
        "needs_refresh": needs_refresh,
        "in_progress": in_progress,
        "failed_slices": failed,
        "last_evaluated_at": _now_iso(),
    }
    log.info("Manifest Summary: %s", manifest["summary"])

