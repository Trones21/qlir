from __future__ import annotations

from typing import Iterable, List, Optional, Union, Sequence
import numpy as _np
import pandas as _pd
import warnings

from qlir.core.counters.multivariate import _maybe_copy, _safe_name
from qlir.core.ops._helpers import ColsLike, _add_columns_from_series_map, _normalize_cols
from qlir.core.ops.non_temporal import with_sign 

# ----------------------------
# Public API
# ----------------------------

def with_diff(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Add first-order difference: x_t - x_{t-periods}
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    diff = out[use_cols].diff(periods)

    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: diff[c] for c in use_cols},
        suffix=suffix or f"diff_{periods}",
    )


def with_pct_change(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    fill_method: Optional[str] = None,   # e.g., "ffill" before pct_change
    clip_inf_to_nan: bool = True,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
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
        pct = pct.replace([_np.inf, -_np.inf], _np.nan)

    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: pct[c] for c in use_cols},
        suffix=suffix or f"pct_{periods}",
    )

def with_log_return(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    epsilon: float = 0.0,               # small offset to guard zeros if desired
    fill_method: Optional[str] = None,
    clip_inf_to_nan: bool = True,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
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
    logx = _np.log(tmp)
    lr = logx - logx.shift(periods)

    if clip_inf_to_nan:
        lr = lr.replace([_np.inf, -_np.inf], _np.nan)


    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: lr[c] for c in use_cols},
        suffix=suffix or f"logret_{periods}",
    )

def with_shift(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    periods: int = 1,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Add shifted copy of columns by 'periods' (positive = past).
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)
    shifted = out[use_cols].shift(periods)
    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: shifted[c] for c in use_cols},
        suffix=suffix or f"shift_{periods}",
    )

# ----------------------------
# Convenience: “bar-to-bar” aliases
# ----------------------------

def with_bar_direction(
    df: _pd.DataFrame,
    col: str,
    *,
    periods: int = 1,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Direction of bar-to-bar change (sign of diff): {-1, 0, +1}
    Example: open_t vs open_{t-1} -> direction(open)
    """
    out, diff_cols = with_diff(df, cols=[col], periods=periods, inplace=inplace)
    (diff_col, ) = diff_cols
    # attach direction of that diff
    out, sign_cols = with_sign(out, cols=[diff_col], suffix=suffix or "direction", inplace=True)
    (sign_col, ) = sign_cols
    return out, tuple([diff_col, sign_col])



# We do this so that those ppl who crazily do from qlir.core.ops.temporal import * 
# will see the public funcs 
__all__ = ["with_bar_direction", "with_diff", "with_shift", "with_pct_change", "with_log_return"]