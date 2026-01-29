from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug
import logging 
log = logging.getLogger(__name__)


__all__ = ["with_counts_running", "with_streaks"]


def with_counts_running(
    df: _pd.DataFrame,
    *,
    group_col: str | None = None,
    rel_col: str,
    out_prefix: str = "count_",
) -> _pd.DataFrame:
    """
    Identify contiguous streaks of identical relation values and compute
    their running lengths.

    A new streak begins when:
    - the value in `rel_col` differs from the previous row, or
    - the value in `group_col` differs from the previous row (if provided).

    For each row, this function assigns:
    - a stable streak identifier (`out_id`)
    - the length so far of the current streak (`out_len`)

    Grouping semantics:
    - If `group_col` is provided, streaks are constrained within groups.
    - If `group_col` is None, streaks may span the entire DataFrame.

    This is a low-level run-length encoding primitive. It does not impose
    domain semantics such as sessions or regimes; grouping behavior is
    entirely caller-defined.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame, ordered as intended for streak detection.
    rel_col : str, default "relation"
        Column whose consecutive identical values define streaks.
    group_col : str | None, default None
        Optional column defining independent streak domains.
    out_id : str, default "streak_id"
        Name of the output column containing streak identifiers.
    out_len : str, default "streak_len"
        Name of the output column containing the running streak length.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with added streak identifier and
        streak length columns.

    Notes
    -----
    - Input row order matters; no sorting is performed.
    - `streak_len` is equivalent to the prefix length of the current run.
    - `streak_id` may be used for downstream grouping or persistence analysis.
    """

    if group_col is not None:
    # Only check group_col if the user passed it
        _ensure_columns(df=df, cols=[rel_col, group_col], caller="with_counts_running")
    else:
        _ensure_columns(df=df, cols=rel_col, caller="with_counts_running")   

    out, ev = df_copy_measured(df=df, label="with_counts_running")
    log_memory_debug(ev=ev, log=log)

    if group_col:
        groups = out[group_col]
    else:
        groups = _pd.Series(0, index=out.index)

    for key in ("above", "below", "touch"):
        mask = out[rel_col].eq(key).astype("int8")
        out[f"{out_prefix}{key}"] = mask.groupby(groups).cumsum().astype("int32")
    return out


def with_streaks(
    df: _pd.DataFrame,
    *,
    rel_col: str,
    group_col: str | None = None,
    out_id: str = "streak_id",
    out_len: str = "streak_len",
) -> _pd.DataFrame:
    """
    Add running counts of relation states within an optional group.

    For each row, this computes cumulative counts of specific relation values
    (e.g. "above", "below", "touch") up to and including the current row.

    Grouping semantics:
    - If `group_col` is provided, counts reset when the group value changes.
    - If `group_col` is None, the entire DataFrame is treated as a single group
      and counts accumulate globally.

    This is a low-level primitive intended for simple state accumulation.
    It does not infer or impose semantic meaning on the grouping column
    (e.g. sessions, symbols, regimes).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame, ordered as intended for running accumulation.
    group_col : str | None, default None
        Optional column defining independent accumulation domains.
    rel_col : str, default "relation"
        Column containing relation labels (e.g. "above", "below", "touch").
    out_prefix : str, default "count_"
        Prefix used for generated output columns.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with additional running count columns:
        `{out_prefix}above`, `{out_prefix}below`, `{out_prefix}touch`.

    Notes
    -----
    - Input row order matters; no sorting is performed.
    - This function is equivalent to a grouped cumulative sum over
      one-hot-encoded relation states.
    """
    if group_col is not None:
        # Only check group_col if the user passed it
        _ensure_columns(df=df, cols=[rel_col, group_col], caller="with_streaks")
    else:
        _ensure_columns(df=df, cols=rel_col, caller="with_streaks")   

    out, ev = df_copy_measured(df=df, label="with_streaks")
    log_memory_debug(ev=ev, log=log)

    boundary_change = out[rel_col].ne(out[rel_col].shift(1))
    if group_col:
        boundary_change |= out[group_col].ne(out[group_col].shift(1))
    out[out_id] = boundary_change.cumsum()
    out[out_len] = out.groupby(out_id, sort=False).cumcount().add(1).astype("int32")
    return out 