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
from qlir.core.counters import univariate
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
import logging
log = logging.getLogger(__name__)

#pd.options.display.float_format = "{:,.6f}".format
pd.set_option("display.float_format", lambda x: f"{x:.17f}")



@telemetry(
    log_path=Path("telemetry/analysis_times.log"),
    console=True,
)
def actual(df: pd.DataFrame):
    logdf(df)
    df = df.loc[:,["open"]]
    df, sma_col= indicators.sma(df, col="open", window=14, decimals=6)
    df[sma_col].round(6)

    logdf(df, from_row_idx=2_500_000, max_rows=25) 
    
    # Direction (of SMA - this is important that we used a smoothed col.  
    # if we used OHLC then we wouldnt get the full "legs", due to noise/volatility)
    df, bar_directions = temporal.with_bar_direction(df, col=sma_col)
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    bar_direction = bar_directions[1]

    # Now we can see the groups (series of 1, -1) 
    logdf(df, max_rows=50, from_row_idx=14)

    # assign the group ids 
    # and 
    for_persistence_df = persistence_analysis_prep(df, bar_direction)
    persistence_analysis(for_persistence_df)
    
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
     

    return



def slope_angle_analysis(df: pd.DataFrame, sma_col: str):
    '''If you want to understand how "slope" of the legs is changing

        Think about how the slope of the track on the rollercoaster:
        1. As you reach the top of the hill the slope DECREASES 
        2. the slope then reaches 0 (flat)
        3. the slope then INCREASES as you transition to the drop
        4. the slope hits a maximum steepness while going down
        5. the slope then DECREASES as you approach the bottom
        6. the slope then reaches 0 (flat)
        7. the slope then INCREASES as you transition to going up  
        8. the slope hits a maximum steepness while going up 
        9. repeat step 1

    '''
    
    # How much change occured 
    df, sma_delta_col = temporal.with_diff(df, cols=sma_col)
    df[sma_delta_col] = df[sma_delta_col].abs()
    
    # How much change occured (another way to get there (no qlir needed)) 
    df["sma_abs_delta"] = (df[sma_col] - df[sma_col].shift(1)).abs() 
    
    # Now compare than amount of change to the amount of change from the previous row

    df["ratio (abs v prev abs)"] = (df["sma_abs_delta"] / df["sma_abs_delta"].shift(1))

def frequency_analysis(df: pd.DataFrame):
    '''
    Note to Self: Maybe this is a QLIR primitive, returns an iterable[NamedDF]'''

    # frequency analysis
    # count events by month - Bucketize (min = 0 events, max=max events in a single month, avid overlaps by aligning/summairze persists event start dt)
    # count events by week - Bucketize
    # Count Events by day Bucketize 


def persistence_analysis(df: pd.DataFrame):
    '''Distribution of event persistence length
    (Use Bucketize)
    '''

def persistence_analysis_prep(df: pd.DataFrame, direction_col: str):
    '''
    Prep for the persistence analysis func
    This could be generic for up/down persistence, but i'll have to think about how to make it more generic 
    '''
    # Add group id - will need for bucketizing/summarization 
    # dfs = buckets.bucketize_zoom_equal_width(df[])
    df, sma_up_leg_col = assign_condition_group_id(df=df, condition_col=direction_col, group_col="sma_up_leg_id")

    # create col for false (assign condition group id only evals on true)
    df["direction_negative"] = df[direction_col].map({False:True})
    df, sma_up_leg_col = assign_condition_group_id(df=df, condition_col="direction_negative", group_col="sma_down_leg_id")
    
    # Get running counters
    df, contig_true_rows = univariate.with_running_true(df, direction_col)
    df, contig_false_rows = univariate.with_bars_since_true(df, direction_col)
    
    # Add Persistence (Max of contig per group id )

    return df

def persistence_analysis_prep_generic(df: pd.DataFrame, condition_col: str , col_name_for_added_group_id_col: str):
    assert df[condition_col].any(), "No True rows for persistence analysis"

    df, group_ids_col = assign_condition_group_id(df=df, condition_col=condition_col, group_col=col_name_for_added_group_id_col)
    df, contig_true_rows = univariate.with_running_true(df, condition_col)

    max_run_col = f"{condition_col}_run_len"
    df[max_run_col] = df.groupby(group_ids_col)[contig_true_rows].transform("max")
    
    return df

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


