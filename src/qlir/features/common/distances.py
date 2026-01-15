from __future__ import annotations

import numpy as _np
import pandas as _pd

from qlir.df.utils import _ensure_columns

__all__ = ["with_zscore", "with_distance"]


def with_distance(
    df: _pd.DataFrame,
    *,
    from_: str,   # baseline (e.g., 'vwap')
    to_: str,     # primary  (e.g., 'close')
    include_pct: bool = True,
    prefix: str | None = None,
    in_place: bool = False,
) -> _pd.DataFrame:
    """
    distance   = to_ - from_
    pct_dist   = (to_ - from_) / from_
    """
    _ensure_columns(df=df, cols=[from_, to_], caller="with_distance")

    out = df if in_place else df.copy()

    # Validate presence
    for col in (from_, to_):
        if col not in out:
            raise KeyError(f"Missing required column: {col}")

    # Keep names separate from Series
    base_name = from_
    ref_name  = to_

    base = _pd.to_numeric(out[base_name], errors="coerce")  # denominator for pct
    ref  = _pd.to_numeric(out[ref_name],  errors="coerce")

    # Column names
    pref = prefix or f"{base_name}_to_{ref_name}"
    dist_col = f"{pref}_dist"
    pct_col  = f"{pref}_pct"

    # Vectorized, index-aligned math
    dist = ref.sub(base)
    out[dist_col] = dist

    if include_pct:
        denom = base.replace(0.0, _np.nan)  # guard div-by-zero
        out[pct_col] = dist.div(denom)

    return out



def with_zscore(
    df: _pd.DataFrame,
    *,
    col: str,
    window: int = 200,
    out_col: str | None = None,
    ddof0: bool = True,
) -> _pd.DataFrame:
    
    _ensure_columns(df=df, cols=col, caller="with_zscore")
    
    out = df.copy()
    out_col = out_col or f"{col}_z"
    sd = out[col].rolling(window, min_periods=max(5, window//5)).std(ddof=0 if ddof0 else 1)
    out[out_col] = out[col] / sd.replace(0.0, _pd.NA)
    return out