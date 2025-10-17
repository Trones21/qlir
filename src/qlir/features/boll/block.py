from __future__ import annotations
import pandas as pd
from ...indicators.boll import add_bollinger
from .bands_touch import add_boll_touch_squeeze_flags

__all__ = ["add_boll_feature_block"]


def add_boll_feature_block(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 20,
    k: float = 2.0,
) -> pd.DataFrame:
    out = add_bollinger(df, close_col=close_col, period=period, k=k)
    out = add_boll_touch_squeeze_flags(out, close_col=close_col)
    return out