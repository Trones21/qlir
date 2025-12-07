from __future__ import annotations
import logging

from qlir.time.iso import now_utc, parse_iso
log = logging.getLogger(__name__)
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from .model import KlineSliceKey, SliceStatus
from .urls import generate_kline_slices
from .fetch import fetch_and_persist_slice
from .time_range import compute_time_range

from qlir.data.core.paths import get_data_root


MANIFEST_FILENAME = "manifest.json"


def _seed_manifest_with_expected_slices(manifest, expected_slices):
    """
    Ensure every expected slice exists in manifest with at least a 'pending' status.
    """
    changed = False
    for s in expected_slices:
        key = s.manifest_key
        if key not in manifest:
            manifest[key] = {
                "status": "pending",
                "slice_id": s.slice_id,
                "first_ts": s.first_ts,
                "last_ts": s.last_ts,
                "requested_at": None,
                "completed_at": None,
                "relative_path": None,
                "n_items": None,
                "error": None,
                "http_status": None,
            }
            changed = True
    return changed


def _compute_missing_slices(
    expected: list[KlineSliceKey],
    manifest: dict,
) -> list[KlineSliceKey]:
    """
    Determine which slices still need work.

    Scheduler-space API:
    - input: expected slices (KlineSliceKey)
    - output: missing slices (KlineSliceKey)
    """
    slices = manifest.get("slices", {})
    missing: list[KlineSliceKey] = []

    for slice_key in expected:
        key = slice_key.composite_key()
        entry = slices.get(key)

        status_str = entry.get("status") if entry else SliceStatus.PENDING.value
        try:
            status = SliceStatus(status_str)
        except ValueError:
            status = SliceStatus.PENDING

        if status in (SliceStatus.PENDING, SliceStatus.FAILED):
            missing.append(slice_key)

    return missing



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
            c. Load manifest.json and determine missing/failed slices.
            d. Fetch + persist each missing slice.
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
    sym_interval_dir = (
        Path(root)
        .joinpath("binance", "klines", "raw", symbol, interval)
        .resolve()
    )
    responses_dir = sym_interval_dir.joinpath("responses")
    manifest_path = sym_interval_dir.joinpath(MANIFEST_FILENAME)

    _ensure_dir(sym_interval_dir)
    _ensure_dir(responses_dir)
    log.info("Saving raw reponses to: %s", responses_dir)
    backoff = 1.0

    while True:
        # TODO: Update _compute_time_range_stub
        min_start_ms, max_end_ms = compute_time_range(symbol=symbol, interval=interval, limit=1000)

        expected_slices = _enumerate_expected_slices(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_ms=min_start_ms,
            end_ms=max_end_ms,
        )

        manifest = _load_manifest(
            manifest_path=manifest_path,
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        log.info(f"Total Expected Slice Count:{len(expected_slices)}")
        return 
    
        if _seed_manifest_with_expected_slices(manifest, expected_slices):
            _save_manifest(manifest_path, manifest)  # persist full universe (for external visualization/stats - no need to see slices in memory, b/c they are all in the manifest.json)

        known_statuses = _extract_known_statuses(manifest)
        missing = _compute_missing_slices(expected_slices, known_statuses)

        if not missing:
            # No work to do; reset backoff and sleep for a bit.
            backoff = 1.0
            log.info(f"No missing slices, sleeping for {poll_interval_sec} seconds")
            time.sleep(poll_interval_sec)
            
            continue

        for slice_key in missing:
            key = slice_key.composite_key()
            try:

                entry = manifest[slice_key]
                entry = manifest["slices"].get(key, {})

                # Skip if another thread is currently handling this 
                # (with stale safeguard to prevent items from being locked forever, in case the proc is killed while fetch is being performed)
                if entry.get("status") == "in_progress" and not is_stale(entry):
                    continue

                # Mark in-progress
                entry["status"] = "in_progress"
                entry["requested_at"] = now_utc().isoformat()
                _save_manifest(manifest_path, manifest)

                meta = fetch_and_persist_slice(
                    slice_key=sliceS_key,
                    data_root=root,
                    responses_dir=responses_dir,
                )
                # Contract for fetch_and_persist_slice:
                # meta = {
                #   "slice_id": str,
                #   "relative_path": "responses/<slice_id>.json",
                #   "http_status": int,
                #   "n_items": int,
                #   "first_ts": int | None,
                #   "last_ts": int | None,
                #   "requested_at": ISO8601 str,
                #   "completed_at": ISO8601 str,
                # }

                
                entry.update(
                    {
                        "slice_id": meta["slice_id"],
                        "relative_path": meta["relative_path"],
                        "status": SliceStatus.OK.value,
                        "http_status": meta.get("http_status", 200),
                        "n_items": meta.get("n_items"),
                        "first_ts": meta.get("first_ts"),
                        "last_ts": meta.get("last_ts"),
                        "requested_at": meta.get("requested_at"),
                        "completed_at": meta.get("completed_at"),
                        "error": None,
                    }
                )
                manifest["slices"][key] = entry

                _update_summary(manifest)
                _save_manifest(manifest_path, manifest)

                # Reset backoff on success
                backoff = 1.0

            except Exception as exc:
                # Mark slice as failed and backoff
                now_iso = _now_iso()
                entry = manifest["slices"].get(key, {})
                entry.update(
                    {
                        "status": SliceStatus.FAILED.value,
                        "error": str(exc),
                        "completed_at": now_iso,
                    }
                )
                manifest["slices"][key] = entry
        
                _update_summary(manifest)
                _save_manifest(manifest_path, manifest)
                log.warning("Exception occured for: %s", entry)
                log.warning(exc)
                time.sleep(backoff)
                backoff = min(backoff * 2.0, max_backoff_sec)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

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


def _load_manifest(
    manifest_path: Path,
    symbol: str,
    interval: str,
    limit: int,
) -> Dict:
    """
    Load manifest.json if present; otherwise create a fresh skeleton.
    """
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # Fresh skeleton
    log.info("Creating fresh manifest because there was no manifest found at: %s", manifest_path)
    return {
        "version": 1,
        "endpoint": "klines",
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "summary": {
            "total_slices": 0,
            "ok_slices": 0,
            "failed_slices": 0,
            "last_evaluated_at": None,
        },
        "slices": {},
    }


def _save_manifest(manifest_path: Path, manifest: Dict) -> None:
    """
    Write manifest.json to disk atomically-ish (write then replace).
    """
    tmp_path = manifest_path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    tmp_path.replace(manifest_path)
    print("Manifest updated: ", manifest_path)


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


# def _compute_missing_slices(
#     expected: List[KlineSliceKey],
#     known_statuses: Dict[str, SliceStatus],
# ) -> List[KlineSliceKey]:
#     missing: List[KlineSliceKey] = []

#     for slice_key in expected:
#         key = slice_key.composite_key()
#         status = known_statuses.get(key, SliceStatus.PENDING)
#         if status in (SliceStatus.PENDING, SliceStatus.FAILED):
#             missing.append(slice_key)

#     return missing


def _update_summary(manifest: Dict) -> None:
    """
    Recompute summary stats based on current slice entries.
    """
    slices = manifest.get("slices", {})
    total = len(slices)
    ok = sum(1 for e in slices.values() if e.get("status") == SliceStatus.OK.value)
    failed = sum(1 for e in slices.values() if e.get("status") == SliceStatus.FAILED.value)

    manifest["summary"] = {
        "total_slices": total,
        "ok_slices": ok,
        "failed_slices": failed,
        "last_evaluated_at": _now_iso(),
    }
    log.info("Manifest Summary Updated: %s", manifest["summary"])
