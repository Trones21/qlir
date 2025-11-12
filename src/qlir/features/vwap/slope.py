from __future__ import annotations
import pandas as pd

__all__ = ["with_vwap_slope"]


def with_vwap_slope(df: pd.DataFrame, *, vwap_col: str = "vwap", out_col: str = "vwap_slope") -> pd.DataFrame:
    out = df.copy()
    out[out_col] = out[vwap_col].astype(float).diff()
    return out