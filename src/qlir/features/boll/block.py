from __future__ import annotations

import pandas as _pd
import logging
log = logging.getLogger(__name__)

from ...indicators.boll import with_bollinger
from ..common import distances, temporal
from .candle_relations import with_candle_line_relations, with_candle_relation_mece

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

    # Regimes
    log.info("Still need to write the boll regimes module")


    # Derivatives
    # skip for now
    log.info("Skipping Boll band temporal derivatives -- i am not sure of exactly what i was going for when i originally wrote this") 
    # out = temporal.temporal_derivatives(out, cols=["boll_mid", "boll_lower", "boll_upper"])


    return out