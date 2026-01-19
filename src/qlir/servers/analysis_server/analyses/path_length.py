


import pandas as pd
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation
from qlir.df.scalars.units import delta_in_pct, delta_in_bps
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
from qlir.df.granularity.to_row_per_time_chunk.time_chunk import to_row_per_time_chunk
from qlir.logging.logdf import logdf
import logging

from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
log = logging.getLogger(__name__)

def path_length_analysis(df: pd.DataFrame):
    df = df.loc[:, ["open", "high", "low"]]
    df = path_length_cols(df)
    
    daily_gran = daily_path_length(df=df, metric_col="path_length_%")
    logdf(daily_gran)
    dists = bucketize_zoom_equal_width(daily_gran["path_length_%_sum"])
    logdf(dists, max_rows=21)

    hourly_gran = hourly_path_length(df=df, metric_col="path_length_%")
    logdf(hourly_gran)
    dists = bucketize_zoom_equal_width(hourly_gran["path_length_%_sum"])
    logdf(dists, max_rows=21)


def path_length_cols(df: pd.DataFrame):
    df["path_length"] = df["high"] - df["low"]
    df["path_length_%"] = delta_in_pct(df["path_length"], df["open"])
    df["path_length_bps"] = delta_in_bps(df["path_length"], df["open"])

    logdf(df)
    return df


def daily_path_length(df, metric_col: str):
    
    dailys = to_row_per_time_chunk(df=df, 
                          ts_col="tz_idx",
                          freq=TimeFreq(count=1, unit=TimeUnit.DAY), 
                          metrics=[MetricSpec(col=metric_col , agg=Aggregation.SUM)]
                          )
    
    return dailys 

def hourly_path_length(df, metric_col: str):
    
    hourlys = to_row_per_time_chunk(df=df, 
                          ts_col="tz_idx",
                          freq=TimeFreq(count=1, unit=TimeUnit.HOUR), 
                          metrics=[MetricSpec(col=metric_col , agg=Aggregation.SUM)]
                          )
    
    return hourlys 