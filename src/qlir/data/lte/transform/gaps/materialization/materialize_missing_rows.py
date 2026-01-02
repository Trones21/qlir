"""
Materialize missing rows on a fixed, wall-clock time axis.

Why wall-clock time?
--------------------
Time is continuous in the real world; markets are not.

Price discovery, information arrival, and risk accumulation occur 24/7,
even when a given market is closed, halted, or otherwise not observable.
Market hours are a property of *observation*, not of time itself.

By materializing rows on a fixed wall-clock interval (e.g. one row per
real-world minute), we preserve the physically correct time axis and make
gaps explicit rather than implicit.

This allows downstream logic to:
- measure gap *size* in real time (minutes / seconds)
- distinguish overnight vs weekend vs (outage or circuit breaker) gaps
  by grouping contiguous materialized rows
- apply policies (carry, interpolate, penalize, reject) without
  compressing or distorting time
- unify 24/7 markets (crypto) and intermittent markets (equities)
  under the same representation

Key principle:
--------------
Missing prices ≠ missing time.

If time did not pass, there should be no row.
If time passed but price was not observed, there must be a row.

This module ONLY materializes time.
It does NOT fill values and makes NO semantic claims.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from qlir.data.lte.transform.gaps.materialization.markers import ROW_MATERIALIZED_COL





def materialize_missing_rows(
    df: pd.DataFrame,
    *,
    interval_s: int,
    timestamp_col: Optional[str] = None,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Materialize missing rows at a fixed wall-clock interval.

    This function:
      - Ensures a dense DatetimeIndex at `interval_s` resolution
      - Inserts rows for missing timestamps
      - Marks inserted rows via an INTERNAL column: `__row_materialized`

    IMPORTANT:
      - This function performs NO value filling.
      - It does NOT declare rows synthetic or backfilled.
      - It does NOT interpret why gaps exist (overnight, weekend, outage, etc.).
      - The internal marker column is an implementation detail and may be
        dropped by downstream policy layers.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame. Must be time-indexed or contain a timestamp column.
    interval_s : int
        Fixed wall-clock interval in seconds (e.g. 60 for 1-minute data).
    timestamp_col : str | None
        If provided, this column will be used as the time index.
    strict : bool
        If True, raises if the DatetimeIndex is not strictly increasing.

    Returns
    -------
    pd.DataFrame
        DataFrame with missing rows materialized and internally marked.
    """

    if df.empty:
        return df.copy()

    # ------------------------------------------------------------------
    # Normalize index
    # ------------------------------------------------------------------
    if timestamp_col is not None:
        if timestamp_col not in df.columns:
            raise KeyError(f"timestamp_col '{timestamp_col}' not found in DataFrame")
        df = df.set_index(timestamp_col)

    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DataFrame must be indexed by a DatetimeIndex")

    if strict and not df.index.is_monotonic_increasing:
        raise ValueError("DatetimeIndex must be strictly increasing")

    df = df.sort_index()

    # ------------------------------------------------------------------
    # Build full wall-clock index
    # ------------------------------------------------------------------
    start = df.index.min()
    end = df.index.max()

    freq = pd.to_timedelta(interval_s, unit="s")
    full_index = pd.date_range(start=start, end=end, freq=freq)

    # Fast path: nothing missing
    if len(full_index) == len(df.index):
        out = df.copy()
        out[ROW_MATERIALIZED_COL] = False
        return out

    # ------------------------------------------------------------------
    # Reindex + mark materialized rows
    # ------------------------------------------------------------------
    out = df.reindex(full_index)

    materialized_mask = ~out.index.isin(df.index)
    out[ROW_MATERIALIZED_COL] = materialized_mask

    return out



