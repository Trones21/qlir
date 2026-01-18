import logging

import pandas as pd

from qlir.core.counters import univariate
from qlir.core.ops import temporal
from qlir.core.registries.columns.registry import ColRegistry
from qlir.core.semantics.events import log_column_event
from qlir.core.semantics.row_derivation import ColumnLifecycleEvent
from qlir.core.types.annotated_df import AnnotatedDataFrame
from qlir.core.types.named_df import NamedDF
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.df.granularity.distributions.persistence import condition_persistence

log = logging.getLogger(__name__)


def persistence_up_legs(df: pd.DataFrame, direction_col: str, trendline_col: str) -> AnnotatedDataFrame:
    '''
    Prep for persistence analysis funcs
    assign group ids 
    add a counter for each group 
    add a column with the max of that count 
    '''
    # Map the 1's to true and add the fillna so that all downstream consumers of this column have a clean bool view
    df["dir_col_up"] = df[direction_col].map({1: True}).astype("boolean").fillna(False)

    # Add group id - will need for bucketizing/summarization 
    df, grp_ids_up_legs_col = assign_condition_group_id(df=df, condition_col="dir_col_up", group_col=f"{trendline_col}_up_leg_id")

    # Get running counters
    df, contig_true_rows = univariate.with_running_true(df, col="dir_col_up")

    # Add Persistence (Max of contig per group id )
    df["up_leg_run_len"] = df.groupby(grp_ids_up_legs_col)[contig_true_rows].transform("max")
    log_column_event(caller="with_bar_direction", ev=ColumnLifecycleEvent(col="up_leg_run_len", event="created"))
    
    # uncomment for comparison (spt check)
    # logdf(df, from_row_idx=22, max_rows=40)

    new_cols = ColRegistry()
    new_cols.declare(key="grp_ids_up_legs_col", column=grp_ids_up_legs_col)
    new_cols.declare(key="dir_col_up", column="dir_col_up")
    new_cols.declare(key="up_leg_run_len", column="up_leg_run_len")

    return AnnotatedDataFrame(df, new_cols)



def persistence_down_legs(df: pd.DataFrame, direction_col: str, trendline_col: str) -> AnnotatedDataFrame:
    '''
    Prep for the persistence analysis func
    assign group ids 
    add a counter for each group 
    add a column with the max of that count 
    '''
    # Map -1 to True and add the fillna so that all downstream consumers of this column have a clean bool view
    df["dir_col_down"] = df[direction_col].map({-1:True}).astype("boolean").fillna(False)

    # Add group id - will need for bucketizing/summarization 
    df, grp_ids_legs_col = assign_condition_group_id(df=df, condition_col="dir_col_down", group_col=f"{trendline_col}_down_leg_id")

    # Get running counters
    df, contig_true_rows = univariate.with_running_true(df, "dir_col_down")
    
    # Add Persistence (Max of contig per group id )
    df["down_leg_run_len"] = df.groupby(grp_ids_legs_col)[contig_true_rows].transform("max")
    
    # uncomment for comparison (spt check)
    # logdf(df, from_row_idx=22, max_rows=40)

    new_cols = ColRegistry()
    new_cols.declare(key="grp_ids_legs_col_down", column=grp_ids_legs_col)
    new_cols.declare(key="dir_col_down", column="dir_col_down")
    new_cols.declare(key="down_leg_run_len", column="down_leg_run_len")

    return AnnotatedDataFrame(df, new_cols)
 




