from __future__ import annotations
import pandas as pd


# Re-export handy blocks
from .features.vwap.block import with_vwap_feature_block
from .features.rsi.block import with_rsi_feature_block
from .features.macd.block import with_macd_feature_block
from .features.boll.block import with_boll_feature_block


__all__ = [
"with_vwap_feature_block",
"with_rsi_feature_block",
"with_macd_feature_block",
"with_boll_feature_block",
]


FEATURE_BLOCKS = {
"vwap": with_vwap_feature_block,
"rsi": with_rsi_feature_block,
"macd": with_macd_feature_block,
"boll": with_boll_feature_block,
}




def apply_feature_block(df: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    fn = FEATURE_BLOCKS.get(name.lower())
    if fn is None:
        raise ValueError(f"Unknown feature block: {name}. Available: {list(FEATURE_BLOCKS)}")
    return fn(df, **kwargs)