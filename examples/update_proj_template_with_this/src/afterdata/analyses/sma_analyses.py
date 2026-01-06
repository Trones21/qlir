from pathlib import Path
import pandas as pd
from qlir import indicators
from qlir.core.ops import temporal
from qlir.logging.logdf import logdf
import qlir.df.reducers.distributions.bucketize.equal_width as buckets
from qlir.core.types.named_df import NamedDF
from afterdata.analysis_funcs.persistence import persistence_analysis
from afterdata.analysis_funcs.frequency import frequency_analysis
from afterdata.etl.LTE import load_cleaned_data
from qlir.telemetry.telemetry import telemetry
import logging 
log = logging.getLogger(__name__)


@telemetry(
    log_path=Path("telemetry/sma_analysis_time.log"),
    console=True,
)
def main():
    df = load_cleaned_data()
    logdf(df)
    df = df.loc[:,["tz_start","open"]]
    df, sma_col= indicators.sma(df, col="open", window=14, decimals=6)
    df[sma_col].round(6)

    # Playing around with some module name ideas
    #trendline_legs.frequency_analysis(df, sma_dir)
    #trendline.persistence_analysis(df, )

    # this might be a decent pattern... all that needs to be passed is a trendline 
    # frequency_analysis(df, sma_col)
    # df2 = persistence_analysis(df, sma_col)
    # logdf(df2)
    # Note the persistence.py also has condition_persistence
    # persistence analysis is a wrapper (both directions) -> prep each df -> condition_persistence

    # to do once the single version refactor is done
    get_all_persistence_dists(src_df=df)
    


def get_all_persistence_dists(src_df):
    '''Look at sma windows
    (Eventually we will probably do 5-60 (for 1min data), but maybe 5 -7 good start ing place to understand the workflow)
    Output parquet? and visualize in Tableau? 
    '''
    out_dfs: list[NamedDF] = []
    for window in range(5,7):
        log.info(f"Preparing sma direction persistence dist for sma lookback of {window}")
        df, sma_col= indicators.sma(src_df, col="open", window=window, decimals=6)
        out_dfs.extend(persistence_analysis(df, sma_col))

    
    logdf(out_dfs, max_rows=2)