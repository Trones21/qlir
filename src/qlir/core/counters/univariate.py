from __future__ import annotations

from typing import Optional

import numpy as _np
import pandas as _pd

from qlir.core.semantics.events import log_column_event
from qlir.core.semantics.row_derivation import ColumnLifecycleEvent

BoolDtype = "boolean"

def _maybe_copy(df: _pd.DataFrame, inplace: bool) -> _pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(*parts: object, sep: str = "__") -> str:
    toks = [str(p) for p in parts if p is not None and str(p) != ""]
    return sep.join(toks)

# def _as_bool_series(s: _pd.Series) -> _pd.Series:
#     if s.dtype == BoolDtype or s.dtype == bool:
#         b = s
#     elif _pd.api.types.is_numeric_dtype(s):
#         b = s.ne(0)   
#     else:
#         b = s.notna()
#     return b.fillna(False).astype(BoolDtype)

def _as_bool_series(s: _pd.Series) -> _pd.Series:
    if not (
        _pd.api.types.is_bool_dtype(s)
        or str(s.dtype) == "boolean"
    ):
        raise TypeError(
            f"Expected boolean column, got dtype={s.dtype}. Column passed was: {s.name}"
        )
    return s.astype("boolean")

def _consecutive_true(mask: _pd.Series) -> _pd.Series:
    m = _as_bool_series(mask)
    groups = (~m).cumsum()
    streak = m.astype("int64").groupby(groups, sort=False).cumsum()
    return streak.astype("Int64")

# ----------------------------
# Public API
# ----------------------------

def with_running_true(
    df: _pd.DataFrame,
    col: str,
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, str]:
    """Consecutive True streak counter for a single boolean column."""
    out = _maybe_copy(df, inplace)
    s = _as_bool_series(out[col])
    name = name or _safe_name(col, "run_true")
    out[name] = _consecutive_true(s)
    log_column_event(caller="with_running_true", ev=ColumnLifecycleEvent(col=name, event="created"))
    return out, name 

def with_bars_since_true(
    df: _pd.DataFrame,
    col: str,
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, str]:
    """Bars since last True in column (NaN before first True)."""
    out = _maybe_copy(df, inplace)
    m = _as_bool_series(out[col]).astype(bool)
    n = len(out)
    idx = _np.arange(n, dtype="int64")
    last_true_idx = _pd.Series(_np.where(m, idx, _np.nan), index=out.index).ffill()
    bars_since = _pd.Series(idx, index=out.index) - last_true_idx
    name = name or _safe_name(col, "bars_since_true")
    out[name] = bars_since.where(~bars_since.isna(), _pd.NA).astype("Int64")
    log_column_event(caller="with_bars_since_true", ev=ColumnLifecycleEvent(col=name, event="created"))
    return out, name
