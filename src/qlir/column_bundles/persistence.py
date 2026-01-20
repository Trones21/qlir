import logging

import pandas as pd

from qlir.core.counters import univariate
from qlir.core.ops import temporal
from qlir.core.registries.columns.registry import ColRegistry
from qlir.core.semantics.events import log_column_event
from qlir.core.registries.columns.lifecycle import ColumnLifecycleEvent
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.core.types.named_df import NamedDF
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.df.granularity.distributions.persistence import condition_persistence

log = logging.getLogger(__name__)


def persistence_up_legs(df: pd.DataFrame, direction_col: str, trendline_col: str) -> AnnotatedDF:
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
    log_column_event(caller="persistence_up_legs", ev=ColumnLifecycleEvent(key="up_leg_run_len", col="up_leg_run_len", event="created"))
    
    # uncomment for comparison (spt check)
    # logdf(df, from_row_idx=22, max_rows=40)

    new_cols = ColRegistry()
    new_cols.add(key="grp_ids_up_legs_col", column=grp_ids_up_legs_col)
    new_cols.add(key="dir_col_up", column="dir_col_up")
    new_cols.add(key="up_leg_run_len", column="up_leg_run_len")

    return AnnotatedDF(df=df, new_cols=new_cols)



def persistence_down_legs(df: pd.DataFrame, direction_col: str, trendline_col: str) -> AnnotatedDF:
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
    new_cols.add(key="grp_ids_legs_col_down", column=grp_ids_legs_col)
    new_cols.add(key="dir_col_down", column="dir_col_down")
    new_cols.add(key="down_leg_run_len", column="down_leg_run_len")

    return AnnotatedDF(df=df, new_cols=new_cols)
 

def persistence(df: pd.DataFrame, condition_col: str , col_name_for_added_group_id_col: str) -> AnnotatedDF:
    assert df[condition_col].any(), "No True rows for persistence analysis"


    #fillna so that all downstream consumers of this column have a clean bool view
    df[condition_col] = df[condition_col].astype("boolean").fillna(False)

    df, group_ids_col = assign_condition_group_id(df=df, condition_col=condition_col, group_col=col_name_for_added_group_id_col)
    df, contig_true_rows = univariate.with_running_true(df, group_ids_col)

    max_run_col = f"{condition_col}_run_len"
    df[max_run_col] = df.groupby(group_ids_col)[contig_true_rows].transform("max")
    log_column_event(caller="persistence", ev=ColumnLifecycleEvent(key="persistence", col=max_run_col, event="created"))

    new_cols = ColRegistry()
    new_cols.add(key="group_ids_col", column=group_ids_col)
    new_cols.add(key="max_run_col", column=max_run_col)
    new_cols.add(key="contig_true_rows", column=contig_true_rows)

    return AnnotatedDF(df=df, new_cols=new_cols)
    
  



