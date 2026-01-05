import pandas as _pd
from qlir.data.agg.engine_old import AggConfig
from qlir.data.agg.manifest import AggManifest
from qlir.data.agg.paths import DatasetPaths


def create_or_update_head(
    *,
    agg: AggManifest,
    paths: DatasetPaths,
    new_frames: list[_pd.DataFrame],
    new_slice_ids: list[str],
    cfg: AggConfig,
) -> None:
    """
    Merge new slices into head.parquet.
    If head reaches batch size, seal part(s).
    Persist manifest atomically.
    """
    head_meta = agg.data.get("head")
    head_df = None

    if head_meta and head_meta["slice_ids"]:
        head_path = paths.agg_parts_dir / "head.parquet"
        head_df = _pd.read_parquet(head_path)

    # Merge Frames
    frames = []
    slice_ids = []

    if head_df is not None:
        frames.append(head_df)
        slice_ids.extend(head_meta["slice_ids"])

    frames.extend(new_frames)
    slice_ids.extend(new_slice_ids)

    out = _pd.concat(frames, ignore_index=True)

    while len(slice_ids) >= cfg.batch_slices:
        part_slice_ids = slice_ids[:cfg.batch_slices]
        part_df = out.iloc[: rows_for_these_slices ]

        part_idx = agg.next_part_index()
        part_name = f"part-{part_idx:06d}.parquet"

        write_parquet_part(...)

        agg.add_part(
            part_filename=f"parts/{part_name}",
            slice_ids=part_slice_ids,
            row_count=len(part_df),
            min_open_time=...,
            max_open_time=...,
        )

        # shrink working set
        slice_ids = slice_ids[cfg.batch_slices:]
        out = out.iloc[len(part_df):]
