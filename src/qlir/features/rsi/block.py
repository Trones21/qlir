from __future__ import annotations
import pandas as pd
from ...indicators.rsi import add_rsi
from .regimes import add_rsi_regime_flags

__all__ = ["add_rsi_feature_block"]


def add_rsi_feature_block(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 14,
) -> pd.DataFrame:
    out = add_rsi(df, close_col=close_col, period=period)
    out = add_rsi_regime_flags(out)
    return out