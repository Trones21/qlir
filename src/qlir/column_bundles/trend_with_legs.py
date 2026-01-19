
import pandas as _pd
from qlir import indicators
from qlir.column_bundles.persistence import persistence_down_legs, persistence_up_legs
from qlir.core.ops import temporal
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.core.types.directional_dfs import DirectionalDFs
import logging
log = logging.getLogger(__name__)

def sma_plus_directional_leg_persistence(df: _pd.DataFrame, window: int) -> DirectionalDFs[AnnotatedDF]:
    
    # Showing use of df_and / unwrap
    sma_adf, sma_col = indicators.sma(df, col="open", window=window, decimals=6).df_and("sma_col")
    sma_adf[sma_col].round(6)

    # Showing use of normal object traversal
    adf = temporal.with_bar_direction(sma_adf, col=sma_col)
    direction_col = adf.new_cols.get_column("sign") 
    
    up_adf = persistence_up_legs(adf.df, direction_col, sma_col)
    down_adf = persistence_down_legs(adf.df, direction_col, sma_col)

    return DirectionalDFs(up=up_adf, down=down_adf)