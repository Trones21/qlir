from __future__ import annotations
from typing import Literal, Optional

import pandas as pd


from qlir.df.granularity.to_row_per_time_chunk._helpers import _validate_ts_ref, groupby_time
from qlir.time.timefreq import TimeFreq
from ..metric_spec import MetricSpec, Aggregation
from ..dtype_guard import _validate_metric_dtype

def to_row_per_time_chunk(
    df: pd.DataFrame,
    *,
    ts_col: str | None = None,
    freq: TimeFreq,
    metrics: list[MetricSpec],
    include_all_wall_clock_chunks: bool
) -> pd.DataFrame:
    """
    Reduce a row-aligned DataFrame to one row per fixed time chunk.

    Parameters
    ----------
    df : pd.DataFrame
        Row-aligned input data.
    ts_col : str | Literal['__index__']
        Timestamp column used for chunking.
    freq : TimeFreq
        Time frequency definition.
    metrics : list[MetricSpec]
        Metrics to compute per time chunk.
    include_src_row_count : bool, default False
        Whether to include the number of source rows per chunk.
    """

    # ---- basic validation --------------------------------------------------

    if ts_col is None:
        if not isinstance(df.index, pd.DatetimeIndex):
            raise TypeError("df.index must be DatetimeIndex when ts_col=None")
    else:
        _validate_ts_ref(df=df, ts_col=ts_col)

    if not isinstance(freq, TimeFreq):
        raise TypeError(
            f"freq must be a TimeFreq instance, got {type(freq).__name__}"
        )

    # ---- validate metrics --------------------------------------------------

    seen_out: set[str] = set()

    for m in metrics:
        if m.col not in df.columns:
            raise KeyError(f"Metric column '{m.col}' not found")

        out = m.resolve_out_name()
        if out in seen_out:
            raise ValueError(f"Duplicate output column name '{out}'")
        seen_out.add(out)

        _validate_metric_dtype(df, m)

    # ---- grouping ----------------------------------------------------------

    # Convert TimeFreq → pandas offset string once
    pandas_freq = freq.as_pandas_str

    grouped = groupby_time(
        df=df,
        ts_col=ts_col,
        freq=pandas_freq,
    )

    result_parts: list[pd.Series] = []

    # ---- structural metric -------------------------------------------------

    s = grouped.size()
    s.name = "src_row_count"
    result_parts.append(s)

    # ---- column metrics ----------------------------------------------------

    for m in metrics:
        out = m.resolve_out_name()

        if m.agg == Aggregation.COUNT_TRUE:
            s = grouped[m.col].sum()
        elif m.agg == Aggregation.MIN:
            s = grouped[m.col].min()
        elif m.agg == Aggregation.MAX:
            s = grouped[m.col].max()
        elif m.agg == Aggregation.FIRST:
            s = grouped[m.col].first()
        elif m.agg == Aggregation.LAST:
            s = grouped[m.col].last()
        elif m.agg == Aggregation.SUM:
            s = grouped[m.col].sum()
        elif m.agg == Aggregation.MEDIAN:
            s = grouped[m.col].median()
        else:
            raise AssertionError(f"Unhandled aggregation {m.agg}")

        s.name = out
        result_parts.append(s)

    # ---- assemble ----------------------------------------------------------

    out_df = pd.concat(result_parts, axis=1)

    if not include_all_wall_clock_chunks:
        out_df = out_df.loc[out_df["src_row_count"] > 0]

    # src_row_count should be zero for the bucekts we created (no src rows)
    if include_all_wall_clock_chunks:
        out_df["src_row_count"] = out_df["src_row_count"].fillna(0).astype(int)

    # also for count_true metrics (otherwise this would be NA, but we likely want zero)
    for m in metrics:
        if m.agg == Aggregation.COUNT_TRUE:
            out_df[m.resolve_out_name()] = (
                out_df[m.resolve_out_name()].fillna(0).astype(int)
            )


    out_df = out_df.reset_index().rename(columns={"index": "dt"})

    return out_df
