from __future__ import annotations

from typing import Iterable, List, Optional, Union, Sequence
import numpy as np
import pandas as pd
import warnings 

Number = Union[int, float]
ColsLike =  Optional[Union[str, Sequence[str]]]

# ----------------------------
# Helpers
# ----------------------------

def _numeric_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def _normalize_cols(df: pd.DataFrame, cols: ColsLike) -> List[str]:
    if cols is None:
        return _numeric_cols(df)
    if isinstance(cols, str):
        cols = [cols]
    valid = [c for c in cols if c in df.columns]
    invalid = [c for c in cols if c not in df.columns]
    if invalid:
        warnings.warn(f"Ignoring missing columns: {invalid}", RuntimeWarning)
    return valid

def _maybe_copy(df: pd.DataFrame, inplace: bool) -> pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(base: str, *parts: Union[str, int]) -> str:
    # join non-empty parts with '__'
    extras = [str(p) for p in parts if p is not None and str(p) != ""]
    return f"{base}__{'__'.join(extras)}" if extras else base


# ----------------------------
# Public API (pointwise ops)
# ----------------------------

def add_diff(
    df: pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add first-order difference: x_t - x_{t-periods}
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)
    for c in use_cols:
        name = _safe_name(c, suffix or f"diff_{periods}")
        out[name] = out[c].diff(periods)
    return out


def add_pct_change(
    df: pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    fill_method: Optional[str] = None,   # e.g., "ffill" before pct_change
    clip_inf_to_nan: bool = True,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add percent change: (x_t / x_{t-periods}) - 1
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    tmp = out[use_cols]
    if fill_method:
        tmp = tmp.fillna(method=fill_method)

    pct = tmp.pct_change(periods=periods)

    if clip_inf_to_nan:
        pct = pct.replace([np.inf, -np.inf], np.nan)

    for c in use_cols:
        name = _safe_name(c, suffix or f"pct_{periods}")
        out[name] = pct[c]
    return out


def add_log_return(
    df: pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    epsilon: float = 0.0,               # small offset to guard zeros if desired
    fill_method: Optional[str] = None,
    clip_inf_to_nan: bool = True,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add log return: ln(x_t + eps) - ln(x_{t-periods} + eps)
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    tmp = out[use_cols]
    if fill_method:
        tmp = tmp.fillna(method=fill_method)

    if epsilon != 0.0:
        tmp = tmp + epsilon

    # log returns = log(x) - log(x.shift)
    logx = np.log(tmp)
    lr = logx - logx.shift(periods)

    if clip_inf_to_nan:
        lr = lr.replace([np.inf, -np.inf], np.nan)

    for c in use_cols:
        name = _safe_name(c, suffix or f"logret_{periods}")
        out[name] = lr[c]
    return out


def add_shift(
    df: pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add shifted copy of columns by 'periods' (positive = past).
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)
    for c in use_cols:
        name = _safe_name(c, suffix or f"shift_{periods}")
        out[name] = out[c].shift(periods)
    return out


def add_sign(
    df: pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    zero_as_zero: bool = True,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add sign of series values: {-1, 0, +1} (or {-1, +1} if zero_as_zero=False).
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    for c in use_cols:
        name = _safe_name(c, suffix or "sign")
        s = np.sign(out[c])
        if not zero_as_zero:
            # map zeros to +1 (or choose your convention)
            s = s.replace(0, 1)
        out[name] = s.astype("Int8") if pd.api.types.is_integer_dtype(s) else s
    return out


def add_abs(
    df: pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add absolute value of series.
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)
    for c in use_cols:
        name = _safe_name(c, suffix or "abs")
        out[name] = out[c].abs()
    return out


# ----------------------------
# Convenience: “bar-to-bar” aliases
# ----------------------------

def add_bar_direction(
    df: pd.DataFrame,
    col: str,
    *,
    periods: int = 1,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Direction of bar-to-bar change (sign of diff): {-1, 0, +1}
    Example: open_t vs open_{t-1} -> direction(open)
    """
    out = add_diff(df, cols=[col], periods=periods, inplace=inplace)
    diff_col = _safe_name(col, f"diff_{periods}")
    # attach direction of that diff
    out = add_sign(out, cols=[diff_col], suffix=suffix or "direction", inplace=True)
    return out
