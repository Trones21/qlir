from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pandas as _pd

from qlir.data.lte.transform.policy.base import FillContext
from qlir.core.constants import DEFAULT_OHLC_COLS
from .blocks import MissingBlock
from .materialization.markers import ROW_MATERIALIZED_COL


def build_fill_context(
    *,
    df: _pd.DataFrame,
    block: MissingBlock,
    ohlc_cols: Tuple[str, str, str, str] = DEFAULT_OHLC_COLS,
    interval_s: int,
    context_window_per_side: int,
    strict: bool = True,
) -> FillContext:
    """
    Build a FillContext for a contiguous missing block.

    This function enforces *hard invariants* to prevent silent
    extrapolation or semantic corruption.

    Raises immediately if any invariant is violated.
    """
    start, end = block.start_idx, block.end_idx


    if context_window_per_side < 0:
        raise ValueError("context_window_per_side must be >= 0")

    # ------------------------------------------------------------------
    # Invariant 1: block must not touch dataset boundaries
    # ------------------------------------------------------------------
    if start == 0 or end == len(df) - 1:
        raise ValueError(
            "Cannot build FillContext for block touching dataset boundary "
            "(would require extrapolation)."
        )

    left_row = df.iloc[start - 1]
    right_row = df.iloc[end + 1]

    # ------------------------------------------------------------------
    # Invariant 2: left and right rows must be REAL (not materialized)
    # ------------------------------------------------------------------
    if left_row[ROW_MATERIALIZED_COL]:
        raise ValueError("Left boundary row is materialized; invalid context.")

    if right_row[ROW_MATERIALIZED_COL]:
        raise ValueError("Right boundary row is materialized; invalid context.")

    # ------------------------------------------------------------------
    # Invariant 3: all rows in block must be materialized
    # ------------------------------------------------------------------
    block_slice = df.iloc[start : end + 1]

    if not block_slice[ROW_MATERIALIZED_COL].all():
        raise ValueError("Block contains non-materialized rows.")

    # ------------------------------------------------------------------
    # Invariant 4: timestamps must be strictly contiguous at interval_s
    # ------------------------------------------------------------------
    if not isinstance(df.index, _pd.DatetimeIndex):
        raise TypeError("DataFrame must be indexed by DatetimeIndex.")

    timestamps = df.index[start : end + 1]

    expected = _pd.date_range(
        start=timestamps[0],
        periods=len(timestamps),
        freq=_pd.to_timedelta(interval_s, unit="s"),
    )

    if strict and not timestamps.equals(expected):
        raise ValueError(
            "Block timestamps are not strictly contiguous at interval_s."
        )

    # ------------------------------------------------------------------
    # Invariant 5: no OHLC values present inside the block
    # ------------------------------------------------------------------
    open_col, high_col, low_col, close_col = ohlc_cols

    ohlc_block = block_slice[[open_col, high_col, low_col, close_col]]

    if strict and not ohlc_block.isna().all().all():
        raise ValueError(
            "Block contains OHLC values; refusing to overwrite observed data."
        )

    left_window = _collect_real_window(
        df=df,
        start_idx=start - 1,
        direction=-1,
        limit=context_window_per_side,
    )

    right_window = _collect_real_window(
        df=df,
        start_idx=end + 1,
        direction=+1,
        limit=context_window_per_side,
    )


    # ------------------------------------------------------------------
    # Build immutable context
    # ------------------------------------------------------------------
    return FillContext(
        left=left_row,
        right=right_row,
        timestamps=timestamps,
        interval_s=interval_s,
        left_window=left_window,
        right_window=right_window
    )


def _collect_real_window(
    *,
    df: _pd.DataFrame,
    start_idx: int,
    direction: int,
    limit: int,
) -> list[_pd.Series]:
    """
    Collect up to `limit` real (non-materialized) rows walking
    forward (direction=+1) or backward (direction=-1).
    """
    rows: list[_pd.Series] = []

    i = start_idx
    while 0 <= i < len(df) and len(rows) < limit:
        row = df.iloc[i]
        if not row[ROW_MATERIALIZED_COL]:
            rows.append(row)
        i += direction

    return rows
