from __future__ import annotations

import pandas as _pd

from ...indicators.rsi import with_rsi
from .regimes import with_rsi_regime_flags

__all__ = ["with_rsi_feature_block"]


def with_rsi_feature_block(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 14,
) -> _pd.DataFrame:
    out = with_rsi(df, close_col=close_col, period=period)
    out = with_rsi_regime_flags(out)
    return out