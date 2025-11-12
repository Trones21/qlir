from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Iterable, Optional

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

def with_running_true_all(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Consecutive bars where ALL of given cols are True."""
    out = _maybe_copy(df, inplace)
    cols = list(cols)
    if not cols:
        raise ValueError("cols must be non-empty")
    mask = _as_bool_series(out[cols[0]])
    for c in cols[1:]:
        mask &= _as_bool_series(out[c])
    out[name or _safe_name("all", "run_true", *cols)] = _consecutive_true(mask)
    return out

def with_running_true_at_least(
    df: pd.DataFrame,
    cols: Iterable[str],
    k: int,
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Consecutive bars where at least k of cols are True."""
    out = _maybe_copy(df, inplace)
    cols = list(cols)
    if not cols:
        raise ValueError("cols must be non-empty")
    if not (1 <= k <= len(cols)):
        raise ValueError(f"k must be in [1, {len(cols)}], got {k}")

    bool_df = pd.DataFrame({c: _as_bool_series(out[c]).astype("int8") for c in cols}, index=out.index)
    cond = (bool_df.sum(axis=1) >= k).astype(BoolDtype)
    out[name or _safe_name("atleast", k, "run_true", *cols)] = _consecutive_true(cond)
    return out

def with_bars_since_any_true(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Bars since last bar where ANY of cols were True."""
    out = _maybe_copy(df, inplace)
    cols = list(cols)
    if not cols:
        raise ValueError("cols must be non-empty")
    mask = _as_bool_series(out[cols[0]])
    for c in cols[1:]:
        mask |= _as_bool_series(out[c])

    n = len(out)
    idx = np.arange(n, dtype="int64")
    last_true_idx = pd.Series(np.where(mask, idx, np.nan), index=out.index).ffill()
    bars_since = pd.Series(idx, index=out.index) - last_true_idx
    out[name or _safe_name("any", "bars_since_true", *cols)] = bars_since.where(~bars_since.isna(), pd.NA).astype("Int64")
    return out
