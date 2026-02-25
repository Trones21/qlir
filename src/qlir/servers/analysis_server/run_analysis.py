



import numpy as np
import pandas as pd
from qlir.core.counters import univariate
from qlir.core.types.named_df import NamedDF
from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
from qlir.df.granularity.to_row_per_time_chunk.time_chunk import to_row_per_time_chunk
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.server import get_clean_data
from qlir.servers.analysis_server.analyses.macd.macd_initial import macd_entry
from qlir.data.resampling.generate_candles import generate_candles_from_1m, minute_range
from qlir.io.writer import write
import logging

from qlir.time.timefreq import TimeFreq
from qlir.utils.df_views.dict import dict_to_df
log = logging.getLogger(__name__)

from qlir.data.lte.transform.gaps.materialization.materialize_missing_rows import (
    materialize_missing_rows,
)

from qlir.time.timeunit import TimeUnit
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation

def main():
    base_df = get_clean_data()

    # Add those missing rows
    full_df = materialize_missing_rows(
        base_df,
        interval_s=60,  # one row per real-world minute
    )

    # Skip the registry, import the builder directly
    df = macd_entry(full_df)

    row_num = df["pyramid_len"].to_numpy().tolist().index(2)

    df["is_frontside"] = (df["macd_dist_color"].isin(["dark_green", "dark_red"]) & df["pyramid_len"].notna())
    df["is_backside"] = (df["macd_dist_color"].isin(["light_green", "light_red"]) & df["pyramid_len"].notna())

    df, frontside_idx = univariate.with_running_true(df=df, col="is_frontside")
    df, backside_idx = univariate.with_running_true(df=df, col="is_backside")

    pyramid_id = "condition_group_id"

    df["frontside_len"] = (
        df.groupby(pyramid_id)[frontside_idx]
        .transform("max")
    )

    df["backside_len"] = (
        df.groupby(pyramid_id)[backside_idx]
        .transform("max")
    )

    # check = df[df[pyramid_id] == 3][
    # ["macd_dist_color", frontside_idx, backside_idx, "frontside_len", "backside_len"]
    # ]

    # logdf(check)

    log.info(f"Total pyramids (incl 2 bar (cross -> 1 dark 1 light -> cross )): {df['condition_group_id'].nunique()}")

    # Anything under this length isnt really tradable
    min_front = 3
    min_back = 3
    df = df.loc[
        (df["frontside_len"] >= min_front) &
        (df["backside_len"]  >= min_back)
    ]
    log.info(f"Pyramids in analysis:{df[pyramid_id].nunique()} min_frontside:{min_front} min_backside:{min_back}")
    
    
    logdf(data=df, max_rows=20,  cols_filter_all_dfs=["close", "ema_fast", "ema_slow","macd",
            "is_histogram_pyramid__run_true", "macd_dist_color",
            "pyramid_len", frontside_idx, backside_idx])
    

    #Materialize the dt
    df = df.assign(dt=df.index)

    pyr_df = to_row_per_event(df=df, event_id_col=pyramid_id, metrics=[
            MetricSpec(col="pyramid_len", agg=Aggregation.MAX), 
            MetricSpec(col="frontside_len", agg=Aggregation.MAX, out="frontside_len"),
            MetricSpec(col="backside_len", agg=Aggregation.MAX, out="backside_len"),
            MetricSpec(col="dt",            agg=Aggregation.FIRST, out="pyr_dt_first"),
                              ])
    logdf(pyr_df)
    len_dists = bucketize_zoom_equal_width(pyr_df["pyramid_len_max"])
    logdf(len_dists, max_rows=50)

    per_diem = to_row_per_time_chunk(df=pyr_df,
                ts_col="pyr_dt_first",
                freq=TimeFreq(count=1, unit=TimeUnit.DAY),
                metrics=[], 
                include_all_wall_clock_chunks=True,
            )

    pyr_count_per_diem = bucketize_zoom_equal_width(per_diem["src_row_count"], int_buckets=True)

    logdf(pyr_count_per_diem, max_rows=300)
    raise
    pyr_df["pyramid_ratio"] = pyr_df["frontside_len"] / pyr_df["backside_len"]
    pyr_df["pyramid_ratio_inv"] = pyr_df["backside_len"] / pyr_df["frontside_len"].replace(0, np.nan)
    
    front_to_back_ratio = bucketize_zoom_equal_width(pyr_df["pyramid_ratio"])
    logdf(front_to_back_ratio, max_rows=50)

    back_to_front_ratio = bucketize_zoom_equal_width(pyr_df["pyramid_ratio_inv"])
    logdf(back_to_front_ratio, max_rows=50)
    raise NotImplementedError()
    
    
    # Optionally remove any sections where data was filled in 
    log.info("NOT REMOVING GENERATED DATA (Filter events later if we decide its necessary)")

    # # Counts of rows in strict pyramids vs not in strict pyramids
    pyramidal_vs_not = {}


    pyr_v_not_count = df["is_histogram_pyramid"].value_counts()  
    pyr_info_dict = pyr_v_not_count.to_dict()  
    stats = {
        "len_min": df["pyramid_len"].min(),
        "len_max": df["pyramid_len"].max(),
        "len_median": df.groupby(pyramid_id)["pyramid_len"].first().median()
    }

    pyr_info_dict.update(stats)
    pyramidal_vs_not["1min"] = pyr_info_dict

    sizes = minute_range(start=5,end=5, include_1m=False)
    dfs_dict = generate_candles_from_1m(df=full_df, out_unit=TimeUnit.MINUTE, out_agg_candle_sizes=sizes)
    


    for k, df in dfs_dict.items():
        tdf = macd_entry(df)
        vc = tdf["is_histogram_pyramid"].value_counts()
        row = vc.to_dict()
        stats = {
        "len_min": tdf["pyramid_len"].min(),
        "len_max": tdf["pyramid_len"].max(),
        "len_median": tdf.groupby(pyramid_id)["pyramid_len"].first().median()
        }
        row.update(stats)
        pyramidal_vs_not[k] = row
        write(tdf, "5min_strict_pyramids.csv")

    log.info(f"pyramidal v not: {pyramidal_vs_not}")
    log.info(type(pyramidal_vs_not["2min"]))
    log.info(pyramidal_vs_not["1min"])

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