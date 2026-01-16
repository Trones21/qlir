from __future__ import annotations

import pandas as pd
from typing import Iterable

from qlir.df.granularity.dtype_guard import _validate_metric_dtype
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation

def to_row_per_event(
    df: pd.DataFrame,
    *,
    event_id_col: str,
    metrics: Iterable[MetricSpec],
    include_src_row_count: bool = False,
) -> pd.DataFrame:
    """
    Reduce a row-aligned DataFrame to one row per event.

    Parameters
    ----------
    df : pd.DataFrame
        Row-aligned input data.
    event_id_col : str
        Column defining event membership.
    metrics : Iterable[MetricSpec]
        Event-level metrics to compute.
    include_src_row_count : bool, default False
        Whether to include the number of source rows per event.
    """

    if event_id_col not in df.columns:
        raise KeyError(f"event_id_col '{event_id_col}' not found in DataFrame")

    metrics = list(metrics)
    if not metrics and not include_src_row_count:
        raise ValueError("No metrics provided and include_src_row_count=False")

    # ---- validation ---------------------------------------------------------

    seen_out_cols: set[str] = set()

    for m in metrics:
        if m.col not in df.columns:
            raise KeyError(f"Metric column '{m.col}' not found in DataFrame")

        out = m.resolve_out_name()
        if out in seen_out_cols:
            raise ValueError(f"Duplicate output column name: '{out}'")
        seen_out_cols.add(out)

        _validate_metric_dtype(df, m)

    # ---- grouping -----------------------------------------------------------

    grouped = df.groupby(event_id_col, sort=False)

    result_frames: list[pd.Series] = []

    # ---- structural metric --------------------------------------------------

    if include_src_row_count:
        src_row_count = grouped.size()
        src_row_count.name = "src_row_count"
        result_frames.append(src_row_count)

    # ---- column metrics -----------------------------------------------------

    for m in metrics:
        out = m.resolve_out_name()

        if m.agg == Aggregation.COUNT_TRUE:
            series = grouped[m.col].sum()
        elif m.agg == Aggregation.MIN:
            series = grouped[m.col].min()
        elif m.agg == Aggregation.MAX:
            series = grouped[m.col].max()
        elif m.agg == Aggregation.FIRST:
            series = grouped[m.col].first()
        elif m.agg == Aggregation.LAST:
            series = grouped[m.col].last()
        elif m.agg == Aggregation.SUM:
            series = grouped[m.col].sum()
        elif m.agg == Aggregation.MEDIAN:
            series = grouped[m.col].median()
        else:
            raise AssertionError(f"Unhandled aggregation: {m.agg}")

        series.name = out
        result_frames.append(series)

    # ---- assemble -----------------------------------------------------------

    result = pd.concat(result_frames, axis=1).reset_index()

    return result
