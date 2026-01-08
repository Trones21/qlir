from __future__ import annotations
import pandas as _pd

from qlir.df.utils import _ensure_columns

__all__ = ["with_vwap_slope"]


def with_vwap_slope(df: _pd.DataFrame, *, vwap_col: str = "vwap", out_col: str = "vwap_slope") -> _pd.DataFrame:

    _ensure_columns(df=df, cols=vwap_col, caller="with_vwap_slope")
    
    out = df.copy()
    out[out_col] = out[vwap_col].astype(float).diff()
    return out