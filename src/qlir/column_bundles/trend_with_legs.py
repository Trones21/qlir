
import pandas as _pd
from qlir import indicators
from qlir.column_bundles.persistence import persistence_down_legs, persistence_up_legs
from qlir.core.ops import temporal
from qlir.core.types.annotated_df import AnnotatedDataFrame
from qlir.core.types.directional_dfs import DirectionalDFs


def sma_plus_directional_leg_persistence(df: _pd.DataFrame, window: int) -> DirectionalDFs[AnnotatedDataFrame]:
    
    sma_adf = indicators.sma(df, col="open", window=window, decimals=6)
    sma_col = sma_adf[1]
    df[sma_col].round(6)

    df, cols = temporal.with_bar_direction(df, col=sma_col)
    direction_col = cols['sign'] #type: ignore 
    
    up_adf = persistence_up_legs(df, direction_col, sma_col)
    down_adf = persistence_down_legs(df, direction_col, sma_col)

    return DirectionalDFs(up=up_adf, down=down_adf)