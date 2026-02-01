


import pandas as pd
from qlir.core.counters import univariate
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.features.macd.histogram import with_colored_histogram
from qlir.features.macd.histogram_pyramid import detect_histogram_pyramids
from qlir.indicators.macd import with_macd
from qlir.logging.logdf import logdf
from qlir.df.scalars.units import delta_in_bps

import logging
log = logging.getLogger(__name__)

def macd_entry(clean_data: pd.DataFrame) -> pd.DataFrame:

    df = with_macd(df=clean_data)
    df["normalized_macd_Δ"] = delta_in_bps(df["macd"], df["close"])
    adf = with_colored_histogram(df=df, fast_col="ema_fast", slow_col="ema_slow")
    dist_color = adf.new_cols.get_column("dist_color")
    adf = detect_histogram_pyramids(df=adf.df, hist_color_col=dist_color)
    pyramid = adf.new_cols.get_column("pyramid")

    # ID per pyramid, intra_idx, pyramid_len
    df, grp_col = assign_condition_group_id(df=adf.df, condition_col=pyramid)
    df, contig_true_rows = univariate.with_running_true(df, col=pyramid)
    df["pyramid_len"] = df.groupby(grp_col)[contig_true_rows].transform("max")
    
    
    log.info(df.columns)
    logdf(df, cols_filter_all_dfs=["close", "ema_fast", "ema_slow","macd", "normalized_macd_Δ", 
                                       dist_color, 
                                       pyramid, 
                                       grp_col, 
                                       contig_true_rows,
                                       "pyramid_len"

                                       ])
    
    return df
