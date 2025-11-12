from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Optional

BoolDtype = "boolean"

def _maybe_copy(df: pd.DataFrame, inplace: bool) -> pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(*parts: object, sep: str = "__") -> str:
    toks = [str(p) for p in parts if p is not None and str(p) != ""]
    return sep.join(toks)

def _as_bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == BoolDtype or s.dtype == bool:
        b = s
    elif pd.api.types.is_numeric_dtype(s):
        b = s.ne(0)
    else:
        b = s.notna()
    return b.fillna(False).astype(BoolDtype)

def _consecutive_true(mask: pd.Series) -> pd.Series:
    m = _as_bool_series(mask)
    groups = (~m).cumsum()
    streak = m.astype("int64").groupby(groups, sort=False).cumsum()
    return streak.astype("Int64")

# ----------------------------
# Public API
# ----------------------------

def with_running_true(
    df: pd.DataFrame,
    col: str,
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Consecutive True streak counter for a single boolean column."""
    out = _maybe_copy(df, inplace)
    s = _as_bool_series(out[col])
    out[name or _safe_name(col, "run_true")] = _consecutive_true(s)
    return out

def with_bars_since_true(
    df: pd.DataFrame,
    col: str,
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Bars since last True in column (NaN before first True)."""
    out = _maybe_copy(df, inplace)
    m = _as_bool_series(out[col]).astype(bool)
    n = len(out)
    idx = np.arange(n, dtype="int64")
    last_true_idx = pd.Series(np.where(m, idx, np.nan), index=out.index).ffill()
    bars_since = pd.Series(idx, index=out.index) - last_true_idx
    out[name or _safe_name(col, "bars_since_true")] = bars_since.where(~bars_since.isna(), pd.NA).astype("Int64")
    return out
