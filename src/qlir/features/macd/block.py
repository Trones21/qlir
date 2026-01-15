from __future__ import annotations

import pandas as _pd

from ...indicators.macd import with_macd
from .crosses import with_macd_cross_flags

__all__ = ["with_macd_feature_block"]


def with_macd_feature_block(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> _pd.DataFrame:
    out = with_macd(df, close_col=close_col, fast=fast, slow=slow, signal=signal)
    out = with_macd_cross_flags(out)
    return out