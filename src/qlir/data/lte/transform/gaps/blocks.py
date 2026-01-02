from __future__ import annotations

from typing import List

import pandas as pd

from .materialization.markers import ROW_MATERIALIZED_COL
from .types import MissingBlock


def find_missing_blocks(df: pd.DataFrame) -> List[MissingBlock]:
    """
    Find contiguous blocks of materialized (missing) rows.

    Assumes `materialize_missing_rows` has already been run and that
    the internal marker column exists.

    Returns blocks as index ranges [start_idx, end_idx], inclusive.
    """
    if ROW_MATERIALIZED_COL not in df.columns:
        raise KeyError(
            f"Expected internal column '{ROW_MATERIALIZED_COL}' not found"
        )

    mask = df[ROW_MATERIALIZED_COL].values

    blocks: List[MissingBlock] = []
    start: int | None = None

    for i, is_missing in enumerate(mask):
        if is_missing and start is None:
            start = i
        elif not is_missing and start is not None:
            blocks.append(MissingBlock(start, i - 1))
            start = None

    # Trailing block
    if start is not None:
        blocks.append(MissingBlock(start, len(mask) - 1))

    return blocks
