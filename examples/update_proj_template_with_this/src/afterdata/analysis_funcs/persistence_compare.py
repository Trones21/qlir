import pandas as pd
from qlir.logging.logdf import logdf
from qlir import indicators
from qlir.core import temporal

from afterdata.analysis_funcs.persistence import persistence_analysis, persistence_analysis_prep_up

def persistence_analysis_compare_methods(df: pd.DataFrame):
    
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
    for_up_pers_df = persistence_analysis_prep_up(df, sma_dir)
    up_legs_dists = persistence_analysis(for_up_pers_df, "sma_up_leg_id", "up_leg_run_len")
    logdf(up_legs_dists, max_rows=25)


        # These two funcs do the whole process (new columns and pivot/reduce)
    # (or less depending on what you pass in)
    # summarize_condition_paths
    # condition_set_persistence_df
    
    # Need to do some work to get summarize_condition_paths right
    # df["up_sma"] = df[sma_dir].map({1:True}).astype("boolean")
    #df["down_sma"] = df[sma_dir].map({-1:True})
    #summarize_condition_paths(df=df, condition_col="up_sma")
    
    #
    # condition_set_persistence_df(df=for_persistence_df, condition_col="up_leg_run_len", condition_set_name="up_legs")
    
    return