# analysis_server/io/parquet_dir.py
"""
Small helpers for treating an agg parquet directory as {sealed chunks + volatile
head}. Sealed chunks are immutable (the agg server appends only to `head.parquet`
and rotates it into a numbered chunk), so a sealed set can be cached by identity.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

HEAD_NAME = "head.parquet"


def classify(
    agg_dir: Path,
    *,
    pattern: str = "*.parquet",
    head_name: str = HEAD_NAME,
) -> tuple[list[Path], Path | None]:
    """Return (sealed_sorted, head_or_None). Sealed = every chunk except head."""
    all_files = sorted(agg_dir.glob(pattern))
    sealed = [p for p in all_files if p.name != head_name]
    head = agg_dir / head_name
    return sealed, (head if head.exists() else None)


def read_concat(paths: list[Path]) -> pd.DataFrame:
    """Raw concat of the given parquet files (no cleaning, no ordering guarantee)."""
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)


def file_key(paths: list[Path]) -> tuple:
    """Identity of a (presumed-immutable) file set, for cache invalidation."""
    return tuple((p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in paths)
