from __future__ import annotations

import pandas as pd


from qlir.df.granularity.to_row_per_time_chunk._helpers import _validate_ts_ref, groupby_time
from qlir.time.timefreq import TimeFreq
from ..metric_spec import MetricSpec, Aggregation
from ..dtype_guard import _validate_metric_dtype

def to_row_per_time_chunk(
    df: pd.DataFrame,
    *,
    ts_col: str,
    freq: TimeFreq,
    metrics: list[MetricSpec],
    include_src_row_count: bool = False,
) -> pd.DataFrame:
    """
    Reduce a row-aligned DataFrame to one row per fixed time chunk.

    Parameters
    ----------
    df : pd.DataFrame
        Row-aligned input data.
    ts_col : str
        Timestamp column used for chunking.
    freq : TimeFreq
        Time frequency definition.
    metrics : list[MetricSpec]
        Metrics to compute per time chunk.
    include_src_row_count : bool, default False
        Whether to include the number of source rows per chunk.
    """

    # ---- basic validation --------------------------------------------------

    _validate_ts_ref(df=df, ts_col=ts_col)

    if not isinstance(freq, TimeFreq):
        raise TypeError(
            f"freq must be a TimeFreq instance, got {type(freq).__name__}"
        )

    if not metrics and not include_src_row_count:
        raise ValueError("No metrics provided and include_src_row_count=False")

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

    if include_src_row_count:
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

    out_df = pd.concat(result_parts, axis=1).reset_index()

    return out_df
