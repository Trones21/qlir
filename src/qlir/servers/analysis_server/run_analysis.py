



import pandas as pd
from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.server import get_clean_data
from qlir.servers.analysis_server.analyses.macd.macd_initial import macd_entry
from qlir.data.resampling.generate_candles import generate_candles_from_1m, minute_range
import logging

from qlir.utils.df_views.dict import dict_to_df
log = logging.getLogger(__name__)

from qlir.data.lte.transform.gaps.materialization.materialize_missing_rows import (
    materialize_missing_rows,
)

from qlir.time.timeunit import TimeUnit


def main():
    base_df = get_clean_data()

    # Add those missing rows
    full_df = materialize_missing_rows(
        base_df,
        interval_s=60,  # one row per real-world minute
    )

    # Skip the registry, import the builder directly
    df = macd_entry(full_df)

    # Optionally remove any sections where data was filled in 
    log.info("NOT REMOVING GENERATED DATA, MUST COMPARE LATER")

    # # Counts of rows in strict pyramids vs not in strict pyramids
    pyramidal_vs_not = {}
    pyramidal_vs_not["1min"] = df["is_histogram_pyramid"].value_counts()

    sizes = minute_range(start=2,end=480, include_1m=False)
    dfs_dict = generate_candles_from_1m(df=full_df, out_unit=TimeUnit.MINUTE, out_agg_candle_sizes=sizes)
    
    log.info(dfs_dict.keys())

    for k, df in dfs_dict.items():
        tdf = macd_entry(df)
        pyramidal_vs_not[k] = tdf["is_histogram_pyramid"].value_counts()

    log.info(pyramidal_vs_not)

    df = (
        pd.DataFrame.from_dict(pyramidal_vs_not, orient="index")
          .reset_index(names="key")
    )

    df["total_candles"] = df[False] + df[True]
    df["%_of_candles_strict_pyr"] = (
        (df[True] / df["total_candles"] * 100).round(2).astype(str) + "%"
    )

    df["%_other"] = (
        (df[False] / df["total_candles"] * 100).round(2).astype(str) + "%"
    )


    logdf(df, max_rows=2000)

    log.info("NEXT Step is to check pcts with n violations")