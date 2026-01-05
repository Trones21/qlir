from __future__ import annotations
import pandas as _pd
from ...indicators.boll import with_bollinger
from .candle_relations import with_candle_line_relations, with_candle_relation_mece
from ..common import temporal, distances
__all__ = ["with_boll_feature_block"]


def with_boll_feature_block(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 20,
    k: float = 2.0,
) -> _pd.DataFrame:
    out = with_bollinger(df, close_col=close_col, period=period, k=k)
    
    ## Descriptive (non derivative)
    out = distances.with_distance(out, from_="boll_lower", to_="boll_upper")

    # Candle Relations
    out = with_candle_relation_mece(out)
    out = with_candle_line_relations(out)

  
    # Derivatives
    out = temporal.temporal_derivatives(out, cols=["boll_mid", "boll_lower", "boll_upper"])


    return out