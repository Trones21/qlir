# src/qlir/core/bar_relations.py
from __future__ import annotations

from typing import Optional, Literal
import pandas as pd

BoolDtype = "boolean"

# ----------------------------
# helpers
# ----------------------------

def _maybe_copy(df: pd.DataFrame, inplace: bool) -> pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(*parts: object, sep: str = "__") -> str:
    toks = [str(p) for p in parts if p is not None and str(p) != ""]
    return sep.join(toks)

def _as_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        raise KeyError(f"column '{col}' not in DataFrame")
    return df[col]

def _shift(s: pd.Series, n: int = 1) -> pd.Series:
    return s.shift(n)

def _safe_bool(s: pd.Series) -> pd.Series:
    # Normalize to pandas nullable boolean and fill NaNs→False for determinism
    if s.dtype != BoolDtype:
        s = s.astype(BoolDtype)
    return s.fillna(False)

# ----------------------------
# basic directional bar relations
# ----------------------------

def with_higher_high(
    df: pd.DataFrame,
    high_col: str = "high",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    h = _as_series(out, high_col)
    out[name or _safe_name(high_col, "higher_high")] = _safe_bool(h > _shift(h))
    return out

def with_lower_low(
    df: pd.DataFrame,
    low_col: str = "low",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    l = _as_series(out, low_col)
    out[name or _safe_name(low_col, "lower_low")] = _safe_bool(l < _shift(l))
    return out

def with_higher_close(
    df: pd.DataFrame,
    close_col: str = "close",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    c = _as_series(out, close_col)
    out[name or _safe_name(close_col, "higher_close")] = _safe_bool(c > _shift(c))
    return out

def with_lower_close(
    df: pd.DataFrame,
    close_col: str = "close",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    c = _as_series(out, close_col)
    out[name or _safe_name(close_col, "lower_close")] = _safe_bool(c < _shift(c))
    return out

def with_higher_open(
    df: pd.DataFrame,
    open_col: str = "open",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    o = _as_series(out, open_col)
    out[name or _safe_name(open_col, "higher_open")] = _safe_bool(o > _shift(o))
    return out

def with_lower_open(
    df: pd.DataFrame,
    open_col: str = "open",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    o = _as_series(out, open_col)
    out[name or _safe_name(open_col, "lower_open")] = _safe_bool(o < _shift(o))
    return out

# ----------------------------
# inside / outside bars
# ----------------------------

def with_inside_bar(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    *,
    name: Optional[str] = None,
    inclusive: Literal["both", "strict"] = "both",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Inside bar: current range is within prior range.
    - inclusive="both": high_t <= high_{t-1} and low_t >= low_{t-1}
    - inclusive="strict": high_t <  high_{t-1} and low_t >  low_{t-1}
    """
    out = _maybe_copy(df, inplace)
    h, l = _as_series(out, high_col), _as_series(out, low_col)
    if inclusive == "both":
        cond = (h <= _shift(h)) & (l >= _shift(l))
    elif inclusive == "strict":
        cond = (h <  _shift(h)) & (l >  _shift(l))
    else:
        raise ValueError("inclusive must be 'both' or 'strict'")
    out[name or _safe_name("inside_bar")] = _safe_bool(cond)
    return out

def with_outside_bar(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    *,
    name: Optional[str] = None,
    inclusive: Literal["both", "strict"] = "both",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Outside bar: current range engulfs prior range.
    - inclusive="both": high_t >= high_{t-1} and low_t <= low_{t-1}
    - inclusive="strict": high_t >  high_{t-1} and low_t <  low_{t-1}
    """
    out = _maybe_copy(df, inplace)
    h, l = _as_series(out, high_col), _as_series(out, low_col)
    if inclusive == "both":
        cond = (h >= _shift(h)) & (l <= _shift(l))
    elif inclusive == "strict":
        cond = (h >  _shift(h)) & (l <  _shift(l))
    else:
        raise ValueError("inclusive must be 'both' or 'strict'")
    out[name or _safe_name("outside_bar")] = _safe_bool(cond)
    return out

# ----------------------------
# body / color and simple patterns
# ----------------------------

def with_bullish_bar(
    df: pd.DataFrame,
    open_col: str = "open",
    close_col: str = "close",
    *,
    name: Optional[str] = None,
    allow_equal: bool = False,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Bullish bar (close > open). If allow_equal=True, uses >=.
    """
    out = _maybe_copy(df, inplace)
    o, c = _as_series(out, open_col), _as_series(out, close_col)
    cond = (c >= o) if allow_equal else (c > o)
    out[name or _safe_name("bullish_bar")] = _safe_bool(cond)
    return out

def with_bearish_bar(
    df: pd.DataFrame,
    open_col: str = "open",
    close_col: str = "close",
    *,
    name: Optional[str] = None,
    allow_equal: bool = False,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Bearish bar (close < open). If allow_equal=True, uses <=.
    """
    out = _maybe_copy(df, inplace)
    o, c = _as_series(out, open_col), _as_series(out, close_col)
    cond = (c <= o) if allow_equal else (c < o)
    out[name or _safe_name("bearish_bar")] = _safe_bool(cond)
    return out

# ----------------------------
# range / expansion utilities
# ----------------------------

def with_true_range(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    *,
    name: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Wilder's True Range:
      TR_t = max( high_t - low_t, abs(high_t - close_{t-1}), abs(low_t - close_{t-1}) )
    """
    out = _maybe_copy(df, inplace)
    h, l, c = _as_series(out, high_col), _as_series(out, low_col), _as_series(out, close_col)
    prev_c = _shift(c)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    out[name or _safe_name("true_range")] = tr
    return out

def with_range_expansion_vs_prev(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    *,
    name: Optional[str] = None,
    method: Literal["highlow", "tr"] = "highlow",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Range expansion vs previous bar:
    - method="highlow": (high-low)_t > (high-low)_{t-1}
    - method="tr":      TR_t > TR_{t-1}   (uses with_true_range)
    """
    out = _maybe_copy(df, inplace)
    if method == "highlow":
        rng = _as_series(out, high_col) - _as_series(out, low_col)
    elif method == "tr":
        tmp = with_true_range(out, high_col=high_col, low_col=low_col, name="__TR__", inplace=False)
        rng = tmp["__TR__"]
    else:
        raise ValueError("method must be 'highlow' or 'tr'")

    cond = rng > _shift(rng)
    out[name or _safe_name("range_expansion", method)] = _safe_bool(cond)
    if "__TR__" in out.columns:
        out = out.drop(columns="__TR__")
    return out
