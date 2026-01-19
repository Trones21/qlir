


import pandas as pd

from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
from qlir.logging.logdf import logdf
from qlir.column_bundles.excursion import excursion


def mae(df: pd.DataFrame):
    df_mae_up = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.UP, mae_or_mfe=ExcursionType.MAE)
    df_mae_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MAE)


def mae_dists(df: pd.DataFrame):
    logdf(df)
    df_mae_up, cols_up = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.UP, mae_or_mfe=ExcursionType.MAE)
    logdf(df_mae_up)

   #  bucketize_zoom_equal_width()



    df_mae_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MAE)

    return 




def mae_in_bps(df: pd.DataFrame):
    return 