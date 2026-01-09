from pathlib import Path
import pandas as pd
from qlir import indicators
from qlir.core.ops import temporal
from qlir.logging.logdf import logdf
import qlir.df.reducers.distributions.bucketize.lossy.equal_width as buckets
from qlir.core.types.named_df import NamedDF
from qlir.core.types.keep_cols import KeepCols
from afterdata.etl.LTE import load_cleaned_data
from qlir.telemetry.telemetry import telemetry
import logging 
log = logging.getLogger(__name__)




def dists_bounds_rename(dists: list[NamedDF], upper_rename: str, lower_rename: str) -> list[NamedDF]:
    for dist in dists:
        dist.df = dist.df.rename(columns={"upper": upper_rename})
        dist.df = dist.df.rename(columns={"lower": lower_rename})
    
    return dists

# @telemetry(
#     log_path=Path("telemetry/sma_analysis_time.log"),
#     console=True,
# )
def main():
    df = load_cleaned_data()
    df = df.loc[:,["tz_start","open", "high", "low", "close"]]

    with_arp, arp_col = indicators.arp(df, keep=KeepCols.ALL ,window=14)
    # top_ten = with_arp.nlargest(10, "hi_lo_rng_pct")
    # logdf(top_ten, max_rows=50)
    logdf(with_arp)
    log.info("Actual Bar Ranges (non-smoothed)")
    with_arp["abs_rng_as_bps"] = with_arp["hi_lo_rng_pct"] * 100
    dists = buckets.bucketize_zoom_equal_width(with_arp["abs_rng_as_bps"], int_buckets=False, human_friendly_fmt=True)
    dists_bounds_renamed = dists_bounds_rename(dists=dists, upper_rename="Upper (bps)", lower_rename="Lower (bps)")    
    logdf(dists_bounds_renamed, max_rows=25)


def arp_main():
    df = load_cleaned_data()
    df = df.loc[:,["tz_start","open", "high", "low"]]
    with_arp, arp_col = indicators.arp(df, keep=KeepCols.ALL ,window=14)
    
    log.info("Reminder: ARP is smoothed")
    arp_as_bps = "arp_as_bps"
    with_arp[arp_as_bps] = with_arp[arp_col] * 100
    dists = buckets.bucketize_zoom_equal_width(with_arp[arp_as_bps], int_buckets=False, human_friendly_fmt=True)

    dists[0].df["upper as pct"] = (dists[0].df["upper"] / 10_000).map(lambda x: f"{x:.2%}")
    dists[0].df = dists[0].df.rename(columns={"upper": "upper (bps)"})
    dists[0].df = dists[0].df.rename(columns={"lower": "lower (bps)"})
    logdf(dists, max_rows=21)
   

def dist_bounds_as_pct(dists: list[NamedDF]):
    dists = []
    for dist in dists:
        dist.df = dist.df.rename(columns={"upper": "upper (bps)"})
        dist.df = dist.df.rename(columns={"lower": "lower (bps)"})

