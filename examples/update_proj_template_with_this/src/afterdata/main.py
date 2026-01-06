# candles_from_disk_via_explicit_filepath
from pathlib import Path
from afterdata.analysis_funcs.frequency import frequency_analysis
from afterdata.analysis_funcs.persistence import condition_persistence, persistence_analysis_prep_down, persistence_analysis_prep_up
from afterdata.analysis_funcs.persistence_compare import persistence_analysis_compare_methods
from afterdata.etl.LTE import load_cleaned_data
from afterdata.logging.logging_setup import setup_logging, LogProfile

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.ALL_DEBUG)
from qlir.telemetry.telemetry import telemetry
import pandas as pd
from qlir.logging.logdf import logdf
import qlir.indicators as indicators
from qlir.core.ops import temporal
import logging
log = logging.getLogger(__name__)

#pd.options.display.float_format = "{:,.6f}".format
pd.set_option("display.float_format", lambda x: f"{x:.17f}")



@telemetry(
    log_path=Path("telemetry/analysis_times.log"),
    console=True,
)
def persistence_analysis_walkthrough(df: pd.DataFrame):
    
    logdf(df)
    df = df.loc[:,["tz_start","open"]]
    df, sma_col= indicators.sma(df, col="open", window=14, decimals=6)
    df[sma_col].round(6)

    #logdf(df, from_row_idx=2_500_000, max_rows=25) 
    
    # Direction (of SMA - this is important that we used a smoothed col.  
    # if we used OHLC then we wouldnt get the full "legs", due to noise/volatility)
    df, sma_direction = temporal.with_bar_direction(df, col=sma_col)
    
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    sma_dir = sma_direction[1]

    # Now we can see the groups (series of 1, -1) 
    logdf(df, max_rows=50, from_row_idx=14)

    # persistence_analysis prep only does the prep... 
    for_up_pers_df, _ = persistence_analysis_prep_up(df, sma_dir)
    up_legs_dists = condition_persistence(for_up_pers_df, "sma_up_leg_id", "up_leg_run_len")
    logdf(up_legs_dists, max_rows=25)

    for_down_pers_df, _ = persistence_analysis_prep_down(df, sma_dir)
    down_legs_dist = condition_persistence(for_down_pers_df, "sma_down_leg_id", "down_leg_run_len")
    logdf(down_legs_dist, max_rows=25)

    return

    # note look at row ~40 there is a -1.5e05... thats basically flat 
    # we might want to expand our definition, IMO there are two wys to do this 
    
    # 1 increase the smoothing window
        # CON: this will decrease the sensitivity (later detection of trend)
    
    # 2 add an overlap in each direction 
        # - so basically you allow the SMA slope to go negative for awhile: 
        #   Threshold Ideas (Logic to end group)
        #   - end only if pct change (or cumulative pct change (a new col))
        #   - or maybe a number of bars 
        #   - or maybe an OR combo (end if either threshold is passed)
        # FYI: if you are analysing both driections (which you should be)
        #      then you would need to create two group id columns (b/c there could be overlap, same candles applied to multiple groups)
        #      this is a best practice anyhow
     
    
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
    #clean_data()
    df = load_cleaned_data()
    # persistence_analysis_compare_methods(df)
    frequency_analysis(df)






def on_project_init():
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
    return

if __name__ == "__main__":
    entrypoint()
