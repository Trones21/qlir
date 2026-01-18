


import pandas as pd

from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.servers.analysis_server.analyses.excursion import excursion


def mae_dists(df: pd.DataFrame):
    
    
    df_mae_up = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.UP, mae_or_mfe=ExcursionType.MAE)
    df_mfe_up = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.UP, mae_or_mfe=ExcursionType.MFE)
    
    df_mae_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MAE)
    df_mfe_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MFE)

    return 




def mae_in_bps(df: pd.DataFrame):
    return 