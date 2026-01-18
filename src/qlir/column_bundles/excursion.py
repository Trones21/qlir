import numpy as np
import pandas as pd
from qlir.core.registries.columns.registry import ColRegistry
from qlir.core.semantics.events import log_column_event
from qlir.core.semantics.row_derivation import ColumnLifecycleEvent
from qlir.core.types.annotated_df import AnnotatedDataFrame
from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.df.scalars.units import delta_in_bps
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import _prep
from typing_extensions import assert_never
from enum import StrEnum
import logging
log = logging.getLogger(__name__)


def excursion_wrapper(df: pd.DataFrame, trendname_or_col_prefix:str, direction: Direction, mae_or_mfe: ExcursionType):
    
    dfs, lists_cols = _prep(df, prefix)

    if direction == Direction.UP:
        df_ = dfs[0]
        cols = lists_cols[0]
        leg_id = f"{trendname_or_col_prefix}_{direction.value}_leg_id"
    
    if direction == Direction.DOWN:
        df_ = dfs[1]
        cols = lists_cols[1]
        leg_id = f"{trendname_or_col_prefix}_{direction.value}_leg_id"
        excursion()

def excursion(df: pd.DataFrame, trendname_or_col_prefix:str, leg_id_col: str, direction: Direction, mae_or_mfe: ExcursionType) -> AnnotatedDataFrame:
    """
    Compute per-leg price excursion metrics (MAE or MFE) for directional trend legs.

    This function operates on a pre-segmented price DataFrame containing
    directional trend legs (e.g. SMA up/down legs). For each leg, it computes:

    - Intra-leg index (0-based bar offset within the leg)
    - Total leg length (number of bars)
    - Excursion from the first open price of the leg
    - Excursion expressed in basis points (bps)
    - The row at which the excursion is maximized (per-leg)
    - Normalized position of the excursion from both the start and the end of the leg
    - Percentage-of-leg timing for when the excursion occurs

    All calculations are performed in a fully vectorized manner using
    `groupby(...).transform(...)`, ensuring that per-leg values are broadcast
    back to every row in the leg.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing at minimum:
        - Directional leg identifiers (e.g. `<prefix>_up_leg_id`, `<prefix>_down_leg_id`)
        - OHLC columns: `open`, `high`, `low`

    trendname_or_col_prefix : str
        Prefix used to construct leg-id and output column names.
        Typically corresponds to the trend or indicator name (e.g. `"open_sma_14"`).

    direction : Direction
        Direction of the trend leg to process (`Direction.UP` or `Direction.DOWN`).
        Selects the appropriate leg partition and leg-id column.

    mae_or_mfe : ExcursionType
        Specifies whether the excursion represents MAE or MFE.
        Used strictly for naming and semantic labeling of output columns.

    Returns
    -------
    pd.DataFrame
        A slim DataFrame containing one row per original bar in the selected
        directional legs, enriched with excursion-related columns, including:

        - `<prefix>_<dir>_<exc>_intra_leg_idx`
        - `<prefix>_<dir>_<exc>_leg_of_n_bars`
        - `<prefix>_<dir>_<exc>_grp_1st_open`
        - `<prefix>_<dir>_<exc>_exc`
        - `<prefix>_<dir>_<exc>_exc_bps`
        - `is_<prefix>_<dir>_<exc>_row`
        - Position-from-start / position-from-end metrics
        - Percent-of-leg timing metrics

    Notes
    -----
    - The excursion "row" is identified using `idxmax` on the excursion
      (in bps) within each leg and is marked via a boolean column.
    - Leg lengths are derived as `max(intra_leg_idx) + 1`, ensuring
      correct accounting for 0-based indexing.
    - The function assumes the input DataFrame has already been split
      into valid directional leg segments by upstream logic.
    """
    
    new_cols = ColRegistry()

    excursion_name = f"{trendname_or_col_prefix}_{direction.value}_{mae_or_mfe.value}"
    
    # Mark the intra leg idx
    log.info(leg_id) 
    logdf(df_)
    intra_leg_idx = f'{excursion_name}_intra_leg_idx'
    df_[intra_leg_idx] = df_.groupby(leg_id).cumcount()
    
    df_slim = df_.loc[:,[leg_id, intra_leg_idx, "open", "high", "low"]]
    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=intra_leg_idx, event="created"))
    new_cols.declare(key="intra_leg_idx", column=intra_leg_idx)
    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col="MANY (FILTER)", event="dropped", reason="Excursion filter step: includes str<leg_id>, str<intra_leg_idx>, open, high, low"))
    

    # Get the leg length / max idx - And apply to: [all row in gorup, new col]
    leg_max_idx = f"{excursion_name}_leg_max_idx"
    leg_of_n_bars = f"{excursion_name}_leg_of_n_bars"
    df_slim[leg_max_idx] = (
        df_slim.groupby(leg_id)[intra_leg_idx]
        .transform("last")
    )
    df_slim[leg_of_n_bars] = df_slim[leg_max_idx] + 1

    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=leg_max_idx, event="created"))
    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=leg_of_n_bars, event="created"))
    new_cols.declare(key="leg_max_idx", column=leg_max_idx)
    new_cols.declare(key="leg_of_n_bars", column=leg_of_n_bars)

    # Get the first open - And apply to: [all rows in group, new col]
    group_first_open = f"{excursion_name}_grp_1st_open"
    df_slim[group_first_open] = (
        df_slim.groupby(leg_id)["open"]
        .transform("first")
    )

    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=group_first_open, event="created"))
    new_cols.declare(key="group_first_open", column=group_first_open)

    # Calc Exc (also in bps)
    df_slim[f"{excursion_name}_exc"] = df_slim["high"] - df_slim[group_first_open]
    df_slim[f"{excursion_name}_exc_bps"] = delta_in_bps(df_slim[f"{excursion_name}_exc"], df_slim[group_first_open])
    
    # Mark the Exc row for each leg
    mfe_row_idx = df_slim.groupby(leg_id)[f"{excursion_name}_exc_bps"].idxmax()
    df_slim[f"is_{excursion_name}_row"] = False
    df_slim.loc[mfe_row_idx, f"is_{excursion_name}_row"] = True

    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=f"{excursion_name}_exc", event="created"))
    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=f"{excursion_name}_exc_bps", event="created"))
    log_column_event(caller="excursion", ev=ColumnLifecycleEvent(col=f"is_{excursion_name}_row", event="created"))
    new_cols.declare(key="excursion", column=f"{excursion_name}_exc")
    new_cols.declare(key="excursion_bps", column=f"{excursion_name}_exc_bps")
    new_cols.declare(key="is_excursion_row", column=f"is_{excursion_name}_row")


    df_slim, cols_se = from_start_and_end(df=df_slim, prefix=excursion_name, leg_max_idx_col=leg_max_idx, intra_leg_idx_col=intra_leg_idx)
    new_cols.extend(cols_se)

    df_slim, cols_pct = pct_when_excursion_max(df=df_slim, prefix=excursion_name, leg_n_bars_col=leg_of_n_bars)
    new_cols.extend(cols_pct)
    
    return AnnotatedDataFrame(df_slim, new_cols)



def from_start_and_end(df: pd.DataFrame, prefix: str, leg_max_idx_col: str, intra_leg_idx_col: str):
    # MFE/MAE occurs N candles from start
    new_cols = ColRegistry()
    df[f"{prefix}_from_start"] = df[leg_max_idx_col] # no math needed
    
    # MFE/MAE occurs N candles from end
    df[f"{prefix}_from_end"] = df[leg_max_idx_col] - df[intra_leg_idx_col]
    
    log_column_event(caller="from_start_and_end", ev=ColumnLifecycleEvent(col=f"{prefix}_from_end", event="created"))
    log_column_event(caller="from_start_and_end", ev=ColumnLifecycleEvent(col=f"{prefix}_from_start", event="created"))
    new_cols.declare(key="from_start", column=f"{prefix}_from_start")
    new_cols.declare(key="from_end", column=f"{prefix}_from_end")
    return df, new_cols
    

def pct_when_excursion_max(df: pd.DataFrame, prefix: str, leg_n_bars_col: str):

    #“What fraction of the entire realized leg had elapsed when MFE/MAE was first achieved?”
    pct_from_start_col = f"{prefix}_pct_from_start"
    df.loc[:,pct_from_start_col] = df[f"{prefix}_from_start"] / df[leg_n_bars_col]

    pct_from_end_col = f"{prefix}_pct_from_end"
    df.loc[:,pct_from_end_col] = df[f"{prefix}_from_end"] / df[leg_n_bars_col]

    sum_col = f"{prefix}_%_from_sum"
    df[sum_col] = df[pct_from_end_col] + df[pct_from_start_col]
    
    log_column_event(caller="pct_when_excursion_max", ev=ColumnLifecycleEvent(col=pct_from_start_col, event="created"))
    log_column_event(caller="pct_when_excursion_max", ev=ColumnLifecycleEvent(col=pct_from_end_col, event="created"))
    log_column_event(caller="pct_when_excursion_max", ev=ColumnLifecycleEvent(col=sum_col, event="created"))

    new_cols = ColRegistry()
    new_cols.declare(key="idx_pct_from_start", column=pct_from_start_col)
    new_cols.declare(key="idx_pct_from_end", column=pct_from_end_col)
    new_cols.declare(key="idx_pct_sum", column=sum_col)

    return df , new_cols







# # Filter to only the MFE/MAE rows 
# df_mfe = df_slim.loc[df_slim["is_mfe_row"] == True , :].copy()