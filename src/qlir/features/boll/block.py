from __future__ import annotations
import pandas as pd
from ...indicators.boll import add_bollinger
from .candle_relations import add_candle_line_relations, add_candle_relation_mece
from ..common import temporal, distances
__all__ = ["add_boll_feature_block"]


def add_boll_feature_block(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 20,
    k: float = 2.0,
) -> pd.DataFrame:
    out = add_bollinger(df, close_col=close_col, period=period, k=k)
    
    ## Descriptive (non derivative)
    out = distances.add_distance(out, from_="boll_lower", to_="boll_upper")

    # Candle Relations
    out = add_candle_relation_mece(out)
    out = add_candle_line_relations(out)

  
    # Derivatives
    out = temporal.temporal_derivatives(out, cols=["boll_mid", "boll_lower", "boll_upper"])


    return out