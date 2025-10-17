from __future__ import annotations
import pandas as pd
from ...indicators.macd import add_macd
from .crosses import add_macd_cross_flags

__all__ = ["add_macd_feature_block"]


def add_macd_feature_block(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    out = add_macd(df, close_col=close_col, fast=fast, slow=slow, signal=signal)
    out = add_macd_cross_flags(out)
    return out