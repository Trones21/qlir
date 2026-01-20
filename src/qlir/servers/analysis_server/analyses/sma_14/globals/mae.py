


import pandas as pd

from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
from qlir.logging.logdf import logdf
from qlir.column_bundles.excursion import excursion
import logging
log = logging.getLogger(__name__)

def mae(df: pd.DataFrame, leg_id_col: str):
    df_mae_up = excursion(df=df, trendname_or_col_prefix="osma14",leg_id_col=leg_id_col, direction=Direction.UP, mae_or_mfe=ExcursionType.MAE)
    df_mae_down = excursion(df=df, trendname_or_col_prefix="osma14", leg_id_col=leg_id_col, direction=Direction.DOWN, mae_or_mfe=ExcursionType.MAE)


def mae_dists(df: pd.DataFrame, leg_id_col: str):
    logdf(df)
    adf_mae_up = excursion(
        df=df, 
        trendname_or_col_prefix="osma14", 
        leg_id_col= leg_id_col,
        direction=Direction.UP, 
        mae_or_mfe=ExcursionType.MAE
        )
    
    log.info(adf_mae_up.new_cols.items())
    view_cols =adf_mae_up.new_cols.get_columns(["group_first_open", "intra_leg_idx", "leg_max_idx", "excursion_bps"])
    logdf(adf_mae_up.df, max_rows=200, cols_filter_all_dfs=["open", *view_cols])
    

    excursion_bps_col = adf_mae_up.new_cols.get_column("excursion_bps")
    
    # Need to drop to 1 row per leg before bucketizing


    mae_bps_buckets = bucketize_zoom_equal_width()
    logdf(mae_bps_buckets)


    # df_mae_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MAE)

    return 




def mae_in_bps(df: pd.DataFrame):
    return 