from functools import partial
from pandas import DataFrame, Series
from qlir import indicators
from qlir.core.ops import temporal
from qlir.core.types.named_df import NamedDF
from qlir.df.granularity.distributions.persistence import condition_persistence
from qlir.df.granularity.summarize_condition_path import summarize_condition_paths
from qlir.execution.on_summary.execute import execute_summary
from qlir.execution.on_summary.execution_models import SummaryExecutionModel
from qlir.logging.logdf import logdf
from qlir.column_bundles.persistence import persistence_down_legs, persistence_up_legs
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width

import logging 
log = logging.getLogger(__name__)

def _prep(clean_df: DataFrame, dev_filter: bool = False):
    
    filtered_df = clean_df
    if dev_filter:
        filtered_df = clean_df.iloc[-2000:]
    
    df, sma_cols = indicators.sma(filtered_df, col="open", window=14, decimals=6)
    sma_col = sma_cols[0] #type: ignore (fix later, lol, works but pylance issue (decorator))
    df[sma_col].round(6)

    df, cols = temporal.with_bar_direction(df, col=sma_col)
    direction_col = cols['sign'] #type: ignore 
    
    for_up, up_cols = persistence_up_legs(df, direction_col, sma_col)
    for_down, down_cols = persistence_down_legs(df, direction_col, sma_col)

    return [for_up, for_down], [up_cols, down_cols]

def execution_analysis(df: DataFrame):
    
    dfs, lists_cols = _prep(df)

    df_up = dfs[0]
    up_cols = lists_cols[0]

    df_up_paths = summarize_condition_paths(
        df=df_up,
        condition_col="dir_col_up__run_true",
        group_col="open_sma_14_up_leg_id"
    )

    # logdf(NamedDF(df_up_paths, "df_up_paths"))

    executed = execute_summary(
        df_up_paths,
        model=SummaryExecutionModel.WORST_ENTRY_EXIT_ON_CLOSE,
        direction="up",
    )


    logdf(NamedDF(executed, "executed"))
    
    p90=partial(Series.quantile, q=0.90)
    p95=partial(Series.quantile, q=0.95)
    p99=partial(Series.quantile, q=0.99)
    
    grouped = (executed
               .groupby('bars')
               .agg(
                   leg_count=("bars", "count"),
                   pnl_med=("pnl", "median"),

                   mfe_p90=("mfe", p90),
                   mae_p90=("mae", p90),
                   mae_min=("mae", "min"),
                   mae_max=("mae", "max"),
                   pnl_min=("pnl", "min"),
                   pnl_max=("pnl", "max"),
                   )
                )


    logdf(grouped, max_rows=1000)
    raise NotImplementedError()
    n_bars = executed[executed['bars'] == 3]
    logdf(n_bars, max_rows=450)

    
    dists = bucketize_zoom_equal_width(n_bars['pnl'])
    logdf(dists)