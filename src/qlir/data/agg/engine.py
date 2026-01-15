from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import json
import pandas as _pd

from qlir.data.agg.atomic import atomic_rename, atomic_write_json
from qlir.data.agg.manifest import AggManifest
from qlir.data.agg.paths import DatasetPaths
from qlir.data.agg.schema_binance_klines import load_binance_kline_slice_json

log = logging.getLogger(__name__)

SLICE_OK = ["complete","partial"]
SLICE_STATUS_KEY = "slice_status"


def wait_load_manifest_json_no_serialize(manifest_path: Path) -> dict[str, Any]:
    """
    Load an existing manifest from disk.

    Contract:
    - waits for the manifest to exist, then loads it 
    """
    log.info("Waiting for manifest.json to exist | path=%s", manifest_path)
    
    while True:
        if manifest_path.exists() and manifest_path.stat().st_size > 0:
                break
        log.warning("STILL waiting for manifest.json to exist | path=%s", manifest_path)
        time.sleep(0.2)

    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)

@dataclass(frozen=True)
class RawSliceRef:
    slice_id: str
    start_ms: int  # for ordering (oldest-first). If not available, set to 0.


def iter_raw_ok_slices(raw_manifest: dict[str, Any]) -> Iterable[RawSliceRef]:
    """
    Adapt this to your raw manifest schema.

    Expected-ish shape:
      raw_manifest["slices"] is dict[key -> entry]
      where entry has:
        - slice_status: "complete"
        - slice_id: "e901567f..."   (or derive from key)
        - start_ms: 1610462400000
    """
    slices = raw_manifest.get("slices")
    if not isinstance(slices, dict):
        raise ValueError("raw manifest missing/invalid 'slices' dict")

    log.info("Slices in raw manifest: %s", len(slices))

    for _key, entry in slices.items():
        if not isinstance(entry, dict):
            continue

        status = entry.get(SLICE_STATUS_KEY)
        if status not in SLICE_OK:
            log.info(f"Ignoring slice {_key}, status: {status} ") 
            continue

        h = entry.get("slice_id")
        if not h:
            raise ValueError("raw manifest entry missing slice_id")

        start_ms = int(entry.get("start_ms", 0))
        yield RawSliceRef(slice_id=h, start_ms=start_ms)


def get_slices_needing_to_be_aggregated(
    raw_manifest: dict[str, Any],
    agg_manifest: AggManifest,
) -> list[RawSliceRef]:
    
    eligible = list(iter_raw_ok_slices(raw_manifest))
    
    # 1. Start with sealed slices
    used: set[str] = set(agg_manifest.all_slice_ids())

    # 2. Union in head slices
    used |= set(agg_manifest.head_slice_ids())
    log.info(f"Used: {len(used)}")
    todo = [s for s in eligible if s.slice_id not in used]
    todo.sort(key=lambda s: s.start_ms)  # oldest-first
    return todo


def write_parquet_atomic(df: _pd.DataFrame, final_path: Path) -> None:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
    df.to_parquet(tmp_path, index=False)
    atomic_rename(tmp_path, final_path)


@dataclass
class AggConfig:
    batch_slices: int = 100
    sleep_idle_s: int = 20
    sleep_partial_s: int = 7
    log_every_loop: bool = True
    # How many NEW slices to attempt per poll (controls CPU/IO burst).
    # Correctness does NOT depend on this value.
    ingest_chunk_slices: int = 100


# ----------------------------
# Head + sealing implementation
# ----------------------------

def _head_path(paths: DatasetPaths) -> Path:
    return paths.agg_parts_dir / "head.parquet"


def _get_head_items(agg: AggManifest) -> list[dict[str, Any]]:
    """
    head.items := [{"slice_id": "...", "row_count": 1000}, ...]
    Preserves slice order (oldest-first).
    """
    head = agg.data.get("head")
    if not isinstance(head, dict):
        return []
    items = head.get("items")
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict) and "slice_id" in it and "row_count" in it:
            out.append({"slice_id": str(it["slice_id"]), "row_count": int(it["row_count"])})
    return out


def _set_head_items(
    agg: AggManifest,
    items: list[dict[str, Any]] | None,
    *,
    head_df: _pd.DataFrame | None,
) -> None:
    """
    Update agg.data["head"] (or remove it if empty).
    """
    if not items:
        agg.data.pop("head", None)
        return

    min_ot = max_ot = None
    if head_df is not None and "open_time" in head_df.columns and len(head_df) > 0:
        # head_df may not be sorted yet when called; that's OK for min/max
        min_ot = int(head_df["open_time"].min())
        max_ot = int(head_df["open_time"].max())

    agg.data["head"] = {
        "items": items,
        "row_count": int(len(head_df)) if head_df is not None else None,
        "min_open_time": min_ot,
        "max_open_time": max_ot,
    }


def _next_part_index(agg: AggManifest) -> int:
    parts = agg.data.get("parts", [])
    if not isinstance(parts, list):
        return 1
    return len(parts) + 1


def create_or_update_parquet_chunk(
    *,
    agg: AggManifest,
    paths: DatasetPaths,
    new_frames: list[_pd.DataFrame],
    new_slice_ids: list[str],
    cfg: AggConfig,
) -> None:
    """
    Append new slices into head.parquet.
    While head has >= batch_slices slices, seal a part.
    Rewrite head.parquet to hold the remainder (< batch_slices slices).

    CRITICAL: We preserve slice boundaries by concatenating in slice order,
    slicing rows by per-slice row_count BEFORE sorting each output parquet.
    """
    if not new_frames:
        log.info("create_or_update_parquet_chunk called, but was not passed any new frames")
        return

    paths.agg_parts_dir.mkdir(parents=True, exist_ok=True)

    # Load existing head (if any)
    head_items = _get_head_items(agg)
    head_df = None
    head_pq = _head_path(paths)
    if head_items and head_pq.exists():
        try:
            head_df = _pd.read_parquet(head_pq)
        except Exception as exc:
            # If head is corrupt, safest recovery is to rebuild it from raw JSON
            # by discarding head parquet but keeping head slice IDs.
            log.warning("Failed to read head.parquet; will rebuild | err=%s", exc)
            head_df = None
            try:
                head_pq.unlink(missing_ok=True)
            except Exception:
                pass
    
    # Log error, duplicate slices being added to head
    existing_ids = {it["slice_id"] for it in head_items}
    for sid, df in zip(new_slice_ids, new_frames):
        if sid in existing_ids:
            log.error("[agg] duplicate slices being added to head: %s", sid)
            continue


    # Combine head + new (in slice order)
    combined_items: list[dict[str, Any]] = []
    combined_frames: list[_pd.DataFrame] = []

    if head_df is not None and head_items:
        combined_items.extend(head_items)
        combined_frames.append(head_df)

    for sid, df in zip(new_slice_ids, new_frames):
        combined_items.append({"slice_id": sid, "row_count": int(len(df))})
        combined_frames.append(df)


    combined = _pd.concat(combined_frames, ignore_index=True)

    # Helper to slice off the first K slices worth of rows (preserving boundaries)
    def rows_for_first_k_slices(items: list[dict[str, Any]], k: int) -> int:
        return int(sum(int(it["row_count"]) for it in items[:k]))

    # Seal as many full parts as possible
    while len(combined_items) >= cfg.batch_slices:
        log.info(f"Writing parquet chunk because combined_items: {len(combined_items)} >= batch_slice_size: {cfg.batch_slices}")
        k = cfg.batch_slices
        nrows = rows_for_first_k_slices(combined_items, k)

        part_items = combined_items[:k]
        part_slice_ids = [it["slice_id"] for it in part_items]

        part_df = combined.iloc[:nrows].copy()

        # Deterministic ordering inside the part
        min_ot = max_ot = None
        if "open_time" in part_df.columns and len(part_df) > 0:
            part_df = part_df.sort_values("open_time", kind="mergesort").reset_index(drop=True)
            min_ot = int(part_df["open_time"].iloc[0])
            max_ot = int(part_df["open_time"].iloc[-1])

        part_idx = _next_part_index(agg)
        part_name = f"part-{part_idx:06d}.parquet"
        final_part_path = paths.agg_parts_dir / part_name

        write_parquet_atomic(part_df, final_part_path)

        agg.add_part(
            part_filename=f"parts/{part_name}",
            slice_ids=part_slice_ids,
            row_count=int(len(part_df)),
            min_open_time=min_ot,
            max_open_time=max_ot,
        )

        # Drop sealed rows + items from combined
        combined = combined.iloc[nrows:].reset_index(drop=True)
        combined_items = combined_items[k:]

        log.info(
            "[agg] seal %s | slices=%s rows=%s open_time=[%s,%s] head_remaining_slices=%s",
            part_name,
            len(part_slice_ids),
            len(part_df),
            min_ot,
            max_ot,
            len(combined_items),
        )

    # Now write head (remainder)
    log.info(f"Writing head because combined_items: {len(combined_items)} < batch_slice_size: {cfg.batch_slices}")
    if combined_items:
        head_df2 = combined.copy()
        if "open_time" in head_df2.columns and len(head_df2) > 0:
            head_df2 = head_df2.sort_values("open_time", kind="mergesort").reset_index(drop=True)

        log.debug("write head.parquet")
        write_parquet_atomic(head_df2, head_pq)
        _set_head_items(agg, combined_items, head_df=head_df2)
    else:
        # No head left
        try:
            head_pq.unlink(missing_ok=True)
        except Exception:
            pass
        _set_head_items(agg, None, head_df=None)

    # Persist manifest once at the end (includes new parts + head changes)
    atomic_write_json(paths.agg_manifest_path, agg.data)
    log.debug("manifest updated")


# ------------
# Refresh only head 
# ------------
def refresh_head_from_raw(
    *,
    agg: AggManifest,
    paths: DatasetPaths,
) -> None:
    """this is needed because the most current slice will have already been discovered, but it will have new data after one interval
        e.g. lets say the most current slice is SOLUSDT:1m:<someopentime>:1000
        after the data server persists the first candle (1 candle), (Slice Status Partial) the slice will be added
        but after the data server the second candle (now slice has 2 candles).. well on the normal path the agg server wouldn't do anything
        because there are no new SLICES. 
        
        Invariant:
        - Only slices listed in agg.data["head"]["items"] may grow.
        - Head is rebuilt from raw JSON every daemon loop.
        - Slices not in head are treated as immutable.
        - Historical corruption is handled by full rebuild jobs.

    """
    head_items = _get_head_items(agg)
    if not head_items:
        return

    frames: list[_pd.DataFrame] = []
    new_items: list[dict[str, Any]] = []

    for it in head_items:
        sid = it["slice_id"]
        raw_path = paths.raw_responses_dir / f"{sid}.json"

        try:
            df = load_binance_kline_slice_json(raw_path)
        except Exception as exc:
            log.warning("[agg] failed to refresh head slice %s: %s", sid, exc)
            return  # abort refresh safely

        frames.append(df)
        new_items.append({"slice_id": sid, "row_count": len(df)})

    combined = _pd.concat(frames, ignore_index=True)

    if "open_time" in combined.columns:
        combined = combined.sort_values("open_time", kind="mergesort").reset_index(drop=True)

    write_parquet_atomic(combined, _head_path(paths))
    _set_head_items(agg, new_items, head_df=combined)
    atomic_write_json(paths.agg_manifest_path, agg.data)

    log.debug("[agg] refreshed head (%d slices, %d rows)", len(new_items), len(combined))



# ------------
# Daemon runner
# ------------

def run_agg_daemon(
    paths: DatasetPaths,
    dataset_meta: dict[str, Any],
    cfg: AggConfig,
) -> None:
    paths.agg_root.mkdir(parents=True, exist_ok=True)
    paths.agg_parts_dir.mkdir(parents=True, exist_ok=True)

    while True:
        log.info("inside true")
        raw_manifest = wait_load_manifest_json_no_serialize(paths.raw_manifest_path)
        agg = AggManifest.load_or_init(paths.agg_manifest_path, dataset_meta)


        # 🔥 ALWAYS refresh head first (because current slice is being updated every interval (1s or 1m))
        refresh_head_from_raw(agg=agg, paths=paths)

        todo = get_slices_needing_to_be_aggregated(raw_manifest, agg)

        if cfg.log_every_loop:
            parts = agg.data.get("parts", [])
            head_items = _get_head_items(agg)
            print(
                f"[agg] todo={len(todo)} used={len(agg.all_slice_ids())} "
                f"parts={len(parts) if isinstance(parts, list) else 0} "
                f"head_slices={len(head_items)}"
            )

        if not todo:
            time.sleep(cfg.sleep_idle_s)
            continue

        # Load some new slices this loop
        batch = todo[: cfg.ingest_chunk_slices]
        new_frames: list[_pd.DataFrame] = []
        new_slice_ids: list[str] = []

        for s in batch:
            h = s.slice_id
            raw_path = paths.raw_responses_dir / f"{h}.json"
            try:
                df = load_binance_kline_slice_json(raw_path)
                new_frames.append(df)
                new_slice_ids.append(h)
            except Exception as exc:
                # Record failure in agg manifest (never raw)
                agg.mark_slice_failed(h, f"{type(exc).__name__}: {exc}")
                atomic_write_json(paths.agg_manifest_path, agg.data)
                log.warning("[agg] slice failed | h=%s err=%s", h, exc)
                continue
        
        if new_frames:
            create_or_update_parquet_chunk(
                agg=agg,
                paths=paths,
                new_frames=new_frames,
                new_slice_ids=new_slice_ids,
                cfg=cfg,
            )
            # If we made progress, poll soon (lets us quickly seal head if more arrived)
            print("next iteration")
