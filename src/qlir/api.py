from __future__ import annotations
import pandas as pd


# Re-export handy blocks
from .features.vwap.block import add_vwap_feature_block
from .features.rsi.block import add_rsi_feature_block
from .features.macd.block import add_macd_feature_block
from .features.boll.block import add_boll_feature_block


__all__ = [
"add_vwap_feature_block",
"add_rsi_feature_block",
"add_macd_feature_block",
"add_boll_feature_block",
]


FEATURE_BLOCKS = {
"vwap": add_vwap_feature_block,
"rsi": add_rsi_feature_block,
"macd": add_macd_feature_block,
"boll": add_boll_feature_block,
}




def apply_feature_block(df: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    fn = FEATURE_BLOCKS.get(name.lower())
    if fn is None:
        raise ValueError(f"Unknown feature block: {name}. Available: {list(FEATURE_BLOCKS)}")
    return fn(df, **kwargs)