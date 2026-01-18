import logging

import pandas as _pd

from qlir import indicators
from qlir.core.ops import temporal
from qlir.df.survival.survival_mark_columns import add_columns_for_trend_survival_rates
from qlir.df.survival.survival_stat import SurvivalStat
from qlir.column_bundles.persistence import (
    persistence_down_legs,
    persistence_up_legs,
)

log = logging.getLogger(__name__)

def sma(clean_data: _pd.DataFrame, window: int) -> tuple[_pd.DataFrame | None, bool]:

    df, sma_cols = indicators.sma(clean_data, col="open", window=window, decimals=6)
    sma_col = sma_cols[0] #type: ignore (fix later, lol, works but pylance issue (decorator))
    if not isinstance(sma_col, str):
        return None , True
    df[sma_col].round(6)
    # persistence_dists = persistence_analysis(df, sma_col)

    df, cols = temporal.with_bar_direction(clean_data, col=sma_col)
    
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    direction = cols["sign"]

    for_up_df, up_cols = persistence_up_legs(df, direction, sma_col)
    for_down_df, down_cols = persistence_down_legs(df, direction, sma_col)
    
    # ========================== UpTrends ===============================================
    df_up = for_up_df.loc[:,["tz_start","open", "open_sma_14", "dir_col_up", "dir_col_up__run_true"]]
    
    # Add Column for Boundary
    df_up[f"sma_{window}_up_started"] = df_up["dir_col_up__run_true"] == 1
    
    #Note that these are rough approximations.. i didnt use the exact amounts (just based it off the buckets (but im within 1 or 2 bars))
    # Add columns for persistence (surivival) emits (10 )
    survival_stats = [
        SurvivalStat(bar_count=21, survival_rate="10%"),
        SurvivalStat(bar_count=28, survival_rate="5%"),
        SurvivalStat(bar_count=42, survival_rate="1%"),
        SurvivalStat(bar_count=70, survival_rate="0.1%"),
    ]
    df_up, objs = add_columns_for_trend_survival_rates(
        df=df_up,
        prefix=f"{sma_col}_up",
        trendrun_count_column="dir_col_up__run_true",
        survival_stats=survival_stats 
    )
    # logdf(df_up)
    # log.info(objs)

    # ==================================================================================

    # ========================== DownTrends ===============================================
    df_down = for_down_df.loc[:,["tz_start","open", "open_sma_14", "dir_col_down", "dir_col_down__run_true"]]

    # Add Column for Boundary
    df_down[f"sma_{window}_down_started"] = df_down["dir_col_down__run_true"] == 1

    # Add columns for persistence (surivival) emits (10 )
    survival_stats = [
        SurvivalStat(bar_count=20, survival_rate="10%"),
        SurvivalStat(bar_count=27, survival_rate="5%"),
        SurvivalStat(bar_count=41, survival_rate="1%"),
        SurvivalStat(bar_count=62, survival_rate="0.1%"),
    ]
    df_down, objs = add_columns_for_trend_survival_rates(
        df=df_down,
        prefix=f"{sma_col}_dn",
        trendrun_count_column="dir_col_down__run_true",
        survival_stats=survival_stats 
    )
    # logdf(df_down)
    # log.info(objs)

    # ==================================================================================

    # Just check the base columns and rows are all the same
    shared = df_up.columns.intersection(df_down.columns)
    log.info(f"up_df: {len(df_up)} down_df: {len(df_down)}")
    if len(df_up) != len(df_down):
        raise AssertionError("Up and down dfs shoul be of the same length")

    df_out = df_up.combine_first(df_down)

    return (df_out, False)

