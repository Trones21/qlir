from __future__ import annotations

from typing import Iterable, List, Optional, Union, Sequence
import numpy as np
import pandas as pd
import warnings

from qlir.core.counters.multivariate import _maybe_copy, _safe_name
from qlir.core.ops.helpers import ColsLike, _normalize_cols
from qlir.core.ops.non_temporal import with_sign 

# ----------------------------
# Public API (pointwise ops)
# ----------------------------

def with_diff(
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


def with_pct_change(
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


def with_log_return(
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


def with_shift(
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




# ----------------------------
# Convenience: “bar-to-bar” aliases
# ----------------------------

def with_bar_direction(
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
    out = with_diff(df, cols=[col], periods=periods, inplace=inplace)
    diff_col = _safe_name(col, f"diff_{periods}")
    # attach direction of that diff
    out = with_sign(out, cols=[diff_col], suffix=suffix or "direction", inplace=True)
    return out
