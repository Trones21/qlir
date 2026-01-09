# from afterdata.logging.logging_setup import setup_logging, LogProfile

# # See logging_setup.py for logging options (LogProfile enum) 
# setup_logging(profile=LogProfile.ALL_DEBUG)
import pandas as pd
import qlir.df.reducers.distributions.bucketize.lossy.equal_width as buckets 
from qlir.core.counters import univariate
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.core.ops import temporal
from qlir.core.types.named_df import NamedDF
from qlir.logging.logdf import logdf
import logging
log = logging.getLogger(__name__)


def condition_persistence(df: pd.DataFrame, group_id_col: str, persistence_col: str) -> list[NamedDF]:
    '''Distribution of event persistence length
    (Summarize then Bucketize)
    '''
    if group_id_col not in df.columns:
        raise KeyError(f"group_id_col: {group_id_col} was passed to condition_persistence, but not found in df. df cols include {df.columns}")

    if persistence_col not in df.columns:
        raise KeyError(f"persistence_col: {group_id_col} was passed to condition_persistence, but not found in df. df cols include {df.columns}")
    
    reduced = (
    df
    .groupby(group_id_col, as_index=False)
    .agg(
        start_ts=("tz_start", "first"),
        end_ts=('tz_start', "last"),
        first_open=("open", "first"),
        last_open=("open", "last"),
        run_len=(persistence_col, "max"),
        )
    )
    # Uncomment to show comparing this with the prior step (to spot check)
    # logdf(reduced, max_rows=20)
    
    return buckets.bucketize_zoom_equal_width(reduced["run_len"], int_buckets=True, human_friendly_fmt=True)


def get_legs(df: pd.DataFrame, trendline_col: str):
    if trendline_col not in df.columns:
        
        raise KeyError()
    df, cols = temporal.with_bar_direction(df, col=trendline_col)


def persistence_analysis(df: pd.DataFrame, trendline_col: str):

    df, cols = temporal.with_bar_direction(df, col=trendline_col)
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    direction = cols[1]

    dfs: list[NamedDF] = []

    for_up_pers_df, _ = persistence_analysis_prep_up(df, direction, trendline_col)
    up_legs_dists = condition_persistence(for_up_pers_df, f"{trendline_col}_up_leg_id", "up_leg_run_len")
    up_legs_dist = up_legs_dists[0]
    up_legs_dist.name = f"{trendline_col}_up_leg_run_len"
    dfs.append(up_legs_dist)
    # logdf(up_legs_dists, max_rows=25)

    for_down_pers_df, _ = persistence_analysis_prep_down(df, direction, trendline_col)
    down_legs_dists = condition_persistence(for_down_pers_df, f"{trendline_col}_down_leg_id", "down_leg_run_len")
    down_legs_dist = down_legs_dists[0]
    down_legs_dist.name = f"{trendline_col}_down_leg_run_len"
    dfs.append(down_legs_dist)
    # logdf(down_legs_dists, max_rows=25)

    return dfs

def persistence_analysis_prep_up(df: pd.DataFrame, direction_col: str, trendline_col: str) -> tuple[pd.DataFrame, list[str]]:
    '''
    Prep for the persistence analysis func
    assign group ids 
    add a counter for each group 
    add a column with the max of that count 
    '''
    # Map the 1's to true and add the fillna so that all downstream consumers of this column have a clean bool view
    df["dir_col_up"] = df[direction_col].map({1: True}).astype("boolean").fillna(False)

    # Add group id - will need for bucketizing/summarization 
    df, grp_ids_sma_up_legs_col = assign_condition_group_id(df=df, condition_col="dir_col_up", group_col=f"{trendline_col}_up_leg_id")

    # Get running counters
    df, contig_true_rows = univariate.with_running_true(df, col="dir_col_up")

    # Add Persistence (Max of contig per group id )
    df["up_leg_run_len"] = df.groupby(grp_ids_sma_up_legs_col)[contig_true_rows].transform("max")
    
    # uncomment for comparison (spt check)
    # logdf(df, from_row_idx=22, max_rows=40)
    return df, [grp_ids_sma_up_legs_col, "up_leg_run_len"]


def persistence_analysis_prep_down(df: pd.DataFrame, direction_col: str, trendline_col: str) -> tuple[pd.DataFrame, list[str]]:
    '''
    Prep for the persistence analysis func
    assign group ids 
    add a counter for each group 
    add a column with the max of that count 
    '''
    # Map -1 to True and add the fillna so that all downstream consumers of this column have a clean bool view
    df["direction_negative"] = df[direction_col].map({-1:True}).astype("boolean").fillna(False)

    # Add group id - will need for bucketizing/summarization 
    df, grp_ids_legs_col = assign_condition_group_id(df=df, condition_col="direction_negative", group_col=f"{trendline_col}_down_leg_id")

    # Get running counters
    df, contig_true_rows = univariate.with_running_true(df, "direction_negative")
    
    # Add Persistence (Max of contig per group id )
    df["down_leg_run_len"] = df.groupby(grp_ids_legs_col)[contig_true_rows].transform("max")
    
    # uncomment for comparison (spt check)
    # logdf(df, from_row_idx=22, max_rows=40)
    return df, [grp_ids_legs_col, "down_leg_run_len"]

def persistence_analysis_prep_generic(df: pd.DataFrame, condition_col: str , col_name_for_added_group_id_col: str) -> tuple[pd.DataFrame, str]:
    assert df[condition_col].any(), "No True rows for persistence analysis"


    #fillna so that all downstream consumers of this column have a clean bool view
    df[condition_col] = df[condition_col].astype("boolean").fillna(False)

    df, group_ids_col = assign_condition_group_id(df=df, condition_col=condition_col, group_col=col_name_for_added_group_id_col)
    df, contig_true_rows = univariate.with_running_true(df, group_ids_col)

    max_run_col = f"{condition_col}_run_len"
    df[max_run_col] = df.groupby(group_ids_col)[contig_true_rows].transform("max")
    
    return df, max_run_col