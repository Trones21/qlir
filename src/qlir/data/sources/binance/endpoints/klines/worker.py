from __future__ import annotations
import logging

from qlir.data.sources.binance.endpoints.klines.manifest import MANIFEST_FILENAME, load_manifest, save_manifest, seed_manifest_with_expected_slices, update_manifest_with_classification, validate_manifest
from qlir.time.iso import now_utc, parse_iso
from qlir.utils.str.color import Ansi, colorize
from qlir.utils.time.fmt import format_ts_human
log = logging.getLogger(__name__)
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from .model import REQUIRED_FIELDS, KlineSliceKey, SliceStatus, classify_slices
from .urls import generate_kline_slices
from .fetch import log_requested_slice_size, make_canonical_slice_hash, fetch_and_persist_slice
from .time_range import compute_time_range

from qlir.data.core.paths import get_data_root

IN_PROGRESS_STALE_SEC = 300  # 5 minutes

def is_stale(entry):
    if entry["status"] != "in_progress":
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

        manifest = load_manifest(
            manifest_path=manifest_path,
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        validate_manifest(manifest)
        log.info(f"Total Expected Slice Count:{len(expected_slices)}")
    
        if seed_manifest_with_expected_slices(manifest, expected_slices):
            save_manifest(manifest_path, manifest, f"Updating Manifest with range {format_ts_human(min_start_ms)} , {format_ts_human(max_end_ms)}")  # persist full universe (for external visualization/stats - no need to see slices in memory, b/c they are all in the manifest.json)
        
        
        # known_statuses = _extract_known_statuses(manifest)
        # log.info(f"known_statuses: {len(known_statuses)}")
        
        classified = classify_slices(expected_slices, manifest)

        manifest = update_manifest_with_classification(manifest=manifest, classified=classified)
        save_manifest(manifest_path, manifest, f"Updating Manifest with slice classifications" )

        to_fetch = [
            *classified.missing,
            *classified.partial,
            *classified.needs_refresh,
            *classified.failed
        ]

        log.info(f"{len(to_fetch)} slices to fetch")
        log.debug(to_fetch)

        # No work to do; reset backoff and sleep for a bit.
        if not to_fetch:
            backoff = 1.0
            log.info(f"No missing or partial slices, sleeping for {poll_interval_sec} seconds")
            time.sleep(poll_interval_sec)
            continue
        
        fetch_comp_keys = [skey.canonical_slice_composite_key() for skey in to_fetch]
        for slice_key in to_fetch:
            meta: dict | None = None
            slice_comp_key = slice_key.canonical_slice_composite_key()
            
            if slice_comp_key not in fetch_comp_keys:
                log.debug(f'fetching {slice_comp_key}, but its not in to_fetch {fetch_comp_keys}')
            
            try:
                entry = manifest["slices"].get(slice_comp_key)
            except Exception as exc:
                raise KeyError(f"Unable to find a manifest entry for key: {slice_comp_key}. You have a corrupt manifest or the server passed an incorrect slice key (code error)")
            try:
                # Skip if another proc is currently handling this 
                # (with stale safeguard to prevent items from being locked forever, in case the proc is killed while fetch is being performed)
                if entry.get("status") == "in_progress" and not is_stale(entry):
                    continue

                # Mark in-progress
                entry["status"] = "in_progress"
                entry["requested_at"] = now_utc().isoformat()

                save_manifest(manifest_path, manifest, "slice marked as in progress")

                meta = fetch_and_persist_slice(
                    request_slice_key=slice_key,
                    data_root=root,
                    responses_dir=responses_dir,
                )

                # Check if the slice is a partial slice (or contract out of date)
                n_items = meta.get("n_items")
                
                if n_items is None:
                    raise RuntimeError(
                        "meta.n_items was not returned from fetch_and_persist_slice"
                        "Partial slice logic assumes meta['n_items'] exists"
                    )

                # Its important to why we received a partial slice - this should give us enough info
                int_limit = int(limit)
                int_n_items = int(n_items)
                if int_n_items != int_limit:
                    log.debug(f"slice {slice_comp_key} marked as {colorize("PARTIAL", Ansi.PINK_HOT , Ansi.BOLD)} Limit={int(limit)} n_items={int(n_items)}")
                    log.debug(colorize(f"You may have requested a partial slice.", Ansi.BOLD))
                    log.debug(f"Request url was: {meta.get('url')}")
                    log.debug(f'Slice Requested: {format_ts_human(meta.get("requested_first_open", "KeyError"))} - {format_ts_human(meta.get("requested_last_open", "KeyError"))}')
                    log.debug(f' Slice Received: {format_ts_human(meta.get("actual_first_open", "KeyError"))} - {format_ts_human(meta.get("actual_last_open", "KeyError"))}')
                    log_requested_slice_size({meta.get('url')}) #type:ignore

                    new_status = SliceStatus.PARTIAL
                
                if int_n_items == int_limit:
                    log.debug(f"slice {slice_comp_key} marked as {colorize("SUCCESSFUL", Ansi.GREEN , Ansi.BOLD)}")
                    new_status = SliceStatus.COMPLETE


                entry.update(
                    {
                        "slice_id": meta["slice_id"],
                        "relative_path": meta["relative_path"],
                        "status": new_status.value, #type:ignore
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
                
                entry = add_or_update_entry_meta_contract(entry=entry, expected_fields=REQUIRED_FIELDS)
                manifest["slices"][slice_comp_key] = entry

                _update_summary(manifest)
                save_manifest(manifest_path, manifest, f"fetch slice succeeded - marking as {colorize(new_status, Ansi.BLUE , Ansi.BOLD)}") #type: ignore

                # Reset backoff on success
                backoff = 1.0

            except Exception as exc:
                # Mark slice as failed and backoff
                now_iso = _now_iso()
                entry = manifest["slices"].get(slice_comp_key, {})
                entry.update(
                    {
                        "status": SliceStatus.FAILED.value,
                        "http_status": (
                            meta.get("http_status")
                            if meta is not None
                            else "exception_occured_before_fetch_and_persist_slice_func_returned"
                        ),
                        "error": str(exc),
                        "completed_at": now_iso,
                    }
                )

                entry = add_or_update_entry_meta_contract(entry, expected_fields=REQUIRED_FIELDS)
                manifest["slices"][slice_comp_key] = entry
        
                _update_summary(manifest)
                save_manifest(manifest_path, manifest, f"fetch slice error - marking as {colorize("error", Ansi.RED , Ansi.BOLD)}")
                # log.warning("Exception occured for: %s", entry)
                log.exception("Slice failed: %s", slice_comp_key)
                time.sleep(backoff)
                backoff = min(backoff * 2.0, max_backoff_sec)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def add_or_update_entry_meta_contract(entry, expected_fields):
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
) -> List[KlineSliceKey]:
    """
    Convert the time range into a list of KlineSliceKey objects.
    """
    slices: List[KlineSliceKey] = []
    for s, e in generate_kline_slices(symbol, interval, start_ms, end_ms, limit):
        slices.append(
            KlineSliceKey(
                symbol=symbol,
                interval=interval,
                start_ms=s,
                end_ms=e,
                limit=limit,
            )
        )
    return slices


def _extract_known_statuses(manifest: Dict) -> Dict[str, SliceStatus]:
    """
    Build a mapping of composite_key -> SliceStatus from the manifest.

    Unknown statuses are treated as PENDING.
    """
    mapping: Dict[str, SliceStatus] = {}
    for key, entry in manifest.get("slices", {}).items():
        status_str = entry.get("status") or SliceStatus.PENDING.value
        try:
            status = SliceStatus(status_str)
        except ValueError:
            status = SliceStatus.PENDING
        mapping[key] = status
    return mapping


def _update_summary(manifest: Dict) -> None:
    """
    Recompute summary stats based on current slice entries.
    """
    slices = manifest.get("slices", {})
    total = len(slices)
    complete = sum(1 for e in slices.values() if e.get("status") == SliceStatus.COMPLETE.value)
    partial = sum(1 for e in slices.values() if e.get("status") == SliceStatus.PARTIAL.value)
    failed = sum(1 for e in slices.values() if e.get("status") == SliceStatus.FAILED.value)
    needs_refresh = sum(1 for e in slices.values() if e.get("status") == SliceStatus.NEEDS_REFRESH.value)

    manifest["summary"] = {
        "total_slices": total,
        "complete_slices": complete,
        "partial_slices": partial,
        "needs_refresh": needs_refresh,
        "failed_slices": failed,
        "last_evaluated_at": _now_iso(),
    }
    log.info("Manifest Summary: %s", manifest["summary"])
