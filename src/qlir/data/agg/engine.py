from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from qlir.data.agg.atomic import atomic_rename, atomic_write_json
from qlir.data.agg.manifest import AggManifest
from qlir.data.agg.paths import DatasetPaths
from qlir.data.agg.schema_binance_klines import load_binance_kline_slice_json

SLICE_OK = "complete"

def load_json(path: Path) -> dict[str, Any]:
    import json
    with path.open("r", encoding="utf-8") as f:
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
        - status: "complete"
        - slice_id: "e901567f..."   (or derive from key)
        - start_ms: 1610462400000
    """
    slices = raw_manifest.get("slices")
    if not isinstance(slices, dict):
        raise ValueError("raw manifest missing/invalid 'slices' dict")

    for _key, entry in slices.items():
        if not isinstance(entry, dict):
            continue
        if entry.get("slicestatus") != SLICE_OK:
            continue

        h = entry.get("slice_id")
        if not h:
            # If you don't store slice_id explicitly, you can derive it here
            # from the composite key using the same hashing function used by ingest.
            raise ValueError("raw manifest entry missing slice_id")

        start_ms = int(entry.get("start_ms", 0))
        yield RawSliceRef(slice_id=h, start_ms=start_ms)

def get_slices_needing_to_be_aggregated(
    raw_manifest: dict[str, Any],
    agg_manifest: AggManifest,
) -> list[RawSliceRef]:
    eligible = list(iter_raw_ok_slices(raw_manifest))
    used = agg_manifest.all_slice_ids()

    todo = [s for s in eligible if s.slice_id not in used]

    # Oldest-first (improves locality, rebuild-friendly)
    todo.sort(key=lambda s: s.start_ms)
    return todo

def write_parquet_part(df: pd.DataFrame, tmp_path: Path, final_path: Path) -> None:
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(tmp_path, index=False)
    atomic_rename(tmp_path, final_path)

@dataclass
class AggConfig:
    batch_slices: int = 100
    sleep_idle_s: int = 45
    sleep_partial_s: int = 7
    log_every_loop: bool = True

def run_agg_daemon(
    paths: DatasetPaths,
    dataset_meta: dict[str, Any],
    cfg: AggConfig,
) -> None:
    paths.agg_root.mkdir(parents=True, exist_ok=True)
    paths.agg_parts_dir.mkdir(parents=True, exist_ok=True)

    while True:
        raw_manifest = load_json(paths.raw_manifest_path)
        agg = AggManifest.load_or_init(paths.agg_manifest_path, dataset_meta)

        todo = get_slices_needing_to_be_aggregated(raw_manifest, agg)

        if cfg.log_every_loop:
            print(
                f"[agg] todo_slices={len(todo)} "
                f"used={len(agg.all_slice_ids())} "
                f"parts={len(agg.data.get('parts', []))}"
            )

        if len(todo) < cfg.batch_slices:
            # Not enough to form a part; sleep and poll again
            time.sleep(cfg.sleep_partial_s if todo else cfg.sleep_idle_s)
            continue

        batch = todo[: cfg.batch_slices]
        slice_ids = [s.slice_id for s in batch]

        # Load and concatenate
        frames: list[pd.DataFrame] = []
        for h in slice_ids:
            try:
                raw_path = paths.raw_responses_dir / f"{h}.json"
                df = load_binance_kline_slice_json(raw_path)
                frames.append(df)
            except Exception as exc:
                # Record failure in agg manifest (never raw)
                agg.mark_slice_failed(h, f"{type(exc).__name__}: {exc}")
                # Persist immediately so it survives crashes
                atomic_write_json(paths.agg_manifest_path, agg.data)
                print(f"[agg] slice failed h={h} err={exc}")
                # Skip this slice for now; continue with others
                continue

        if not frames:
            # If everything failed, avoid tight loop
            time.sleep(cfg.sleep_partial_s)
            continue

        out = pd.concat(frames, ignore_index=True)

        # Deterministic ordering inside parts (mechanical)
        if "open_time" in out.columns:
            out = out.sort_values("open_time", kind="mergesort").reset_index(drop=True)
            min_ot = int(out["open_time"].iloc[0])
            max_ot = int(out["open_time"].iloc[-1])
        else:
            min_ot = None
            max_ot = None

        # Choose part filename (monotonic counter)
        part_idx = len(agg.data.get("parts", [])) + 1
        part_name = f"part-{part_idx:06d}.parquet"
        final_part_path = paths.agg_parts_dir / part_name
        tmp_part_path = final_part_path.with_suffix(".parquet.tmp")

        # Write parquet then commit manifest
        write_parquet_part(out, tmp_part_path, final_part_path)

        # IMPORTANT: Only mark slices as used via agg manifest add_part
        # Include only the slice hashes that actually made it into this part:
        # (i.e., the ones we successfully loaded)
        included_hashes = []
        loaded_set = set()
        for df, h in zip(frames, slice_ids):
            # zip is not safe here if failures occurred mid-loop; instead:
            pass
        # Build included_hashes by re-walking batch and checking file existence in failures
        failures = set(agg.data.get("slice_failures", {}).keys())
        for s in batch:
            if s.slice_id in failures:
                continue
            # Only include if we actually loaded it this round; simplest check:
            raw_path = paths.raw_responses_dir / f"{s.slice_id}.json"
            if raw_path.exists():
                included_hashes.append(s.slice_id)

        agg.add_part(
            part_filename=f"parts/{part_name}",
            slice_ids=included_hashes,
            row_count=int(len(out)),
            min_open_time=min_ot,
            max_open_time=max_ot,
        )
        atomic_write_json(paths.agg_manifest_path, agg.data)

        print(
            f"[agg] wrote {part_name} rows={len(out)}"
            f"slices={len(included_hashes)} "
            f"open_time=[{min_ot},{max_ot}]"
        )
