# candles_from_disk_via_explicit_filepath
from pathlib import Path
from afterdata.etl.LTE import clean_data, load_cleaned_data
from afterdata.logging.logging_setup import setup_logging, LogProfile

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.ALL_DEBUG)
from qlir.telemetry.telemetry import telemetry
import pandas as pd
from qlir.logging.logdf import logdf
import qlir.indicators as indicators
import qlir.df.reducers.distributions.bucketize.equal_width as buckets 
from qlir.core.ops import temporal
from qlir.core.ops import non_temporal
import logging
log = logging.getLogger(__name__)

#pd.options.display.float_format = "{:,.6f}".format
pd.set_option("display.float_format", lambda x: f"{x:.17f}")



@telemetry(
    log_path=Path("telemetry/analysis_times.log"),
    console=True,
)
def actual(df: pd.DataFrame):
    df = df.loc[:,["tz_start","open"]]
    df, sma_col= indicators.sma(df, col="open", window=14, decimals=6)
    df[sma_col].round(6)
    df["sma_delta"]
    df["sma_abs_delta"] = (df[sma_col] - df[sma_col].shift(1)).abs()
    
    df["ratio (abs v prev abs)"] = (df["sma_abs_delta"] / df["sma_abs"].shift(1))
    logdf(df, from_row_idx=2_500_000, max_rows=25) 
    
    # Direction (of SMA - this is important that we used a smoothed col.  
    # if we used OHLC then we wouldnt get the full "legs", due to noise/volatility)
    df, bar_direction = temporal.with_bar_direction(df, col=sma_col)
     
    logdf(df, max_rows=50, from_row_idx=14)
    # Add group id for 
    # dfs = buckets.bucketize_zoom_equal_width(df[])
    # logdf(dfs, max_rows=99)
    




    return


def frequency_analysis(df: pd.DataFrame):
    '''Maybe this is a QLIR primitive, returns a iterable[NamedDF]'''

    # frequency analysis
    # count events by month - Bucketize (min = 0 events, max=max events in a single month, avid overlaps by aligning/summairze persists event start dt)
    # count events by week - Bucketize
    # Count Events by day Bucketize 


def persistence_analysis(df: pd.DataFrame):
    '''Distribution of event persistence length
    (Use Bucketize)
    '''
    # Persistence Analysis
    # 


def main(df: pd.DataFrame):
    '''
    Core analysis body.

    - Add indicators / features / signals
    - Join with other datasets
    - Persist intermediate/final results
    - Optionally produce dataviz-friendly tables, plots, etc.
    '''
    # e.g.:
    # df = compute_slope_persistence_stats(df)
    # df.to_parquet("data/processed/slope_persistence.parquet")

    return df  # useful for tests / notebooks


def entrypoint():
    # print(f""""Hello World!
    #         Welcome to your QLIR analysis project, time to get to work!
            
    #         For Binance Data, first run the binance-data-server-arg to store the raw responses on disk.
    #         For Example:
    #         poetry run binance-data-server-arg --endpoint "klines" --symbol "SOLUSDT" --interval "1m" --limit "1000"
          
    #         Then run the binance-agg-server-arg to aggregate that data into parquets.
    #         For Example:
    #         poetry run binance-agg-server --endpoint "klines" --symbol "SOLUSDT" --interval "1m" --limit "1000"
          
    #         Note: You can only run one endpoint/symbol/interval/limit combo per data server or agg server
    #         Check pyproject.toml to see various ways to launch multiple data and agg servers via single poetry commands 

    #         Finally load your data into memory and do data quality checks, then once you are satisfied you can start your analysis!
    #       """)
        
    # Now you should have the data
    # the standard location is ~/qlir_data
    # ensure you have the data with: find . ~/qlir_data/

    # To load into a Dataframe call either:
    # candles_from_disk_via_built_filepath
    # Which takes: the datasource, symbol and resolution
    # this builds the canonical filepath for this dataset
    # and may use the corresponding dataset .meta.json for additional info
    
    # Or pass the filepath for a specific dataset explicitly
    # hint: use ls to find it
    # candles_from_disk_via_explicit_filepath

    # Optional - Step 2 Resample data (custom OHLCV candles)
    # df_resampled = build_custom_candles(df)

    # Enrich and analyze
    # for now, assume df is ready:
    # df = ...  # TODO: integrate your real pipeline, if you have many, you might consider creating a pipelines folder/module instead of placing it all here 
    
    #clean_data()
    df = load_cleaned_data()
    actual(df)




if __name__ == "__main__":
    entrypoint()


