from __future__ import annotations
import pandas as pd

__all__ = ["add_bollinger"]


def add_bollinger(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 20,
    k: float = 2.0,
    out_mid: str = "boll_mid",
    out_upper: str = "boll_upper",
    out_lower: str = "boll_lower",
    in_place: bool = False,
) -> pd.DataFrame:
    out = df if in_place else df.copy()
    close = out[close_col].astype(float)
    mid = close.rolling(period, min_periods=period//2).mean()
    sd = close.rolling(period, min_periods=period//2).std(ddof=0)
    out[out_mid] = mid
    out[out_upper] = mid + k * sd
    out[out_lower] = mid - k * sd
    return out