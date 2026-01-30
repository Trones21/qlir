import numpy as np
import pandas as pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.semantics.events import log_column_event
from qlir.core.registries.columns.lifecycle import ColumnLifecycleEvent
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.df.scalars.units import delta_in_bps
from qlir.df.utils import _ensure_columns
from qlir.logging.logdf import logdf
from typing_extensions import assert_never
from enum import StrEnum
import logging
log = logging.getLogger(__name__)


# def excursion_wrapper(df: pd.DataFrame, trendname_or_col_prefix:str, direction: Direction, mae_or_mfe: ExcursionType):
    
#     dfs, lists_cols = sma

#     if direction == Direction.UP:
#         df_ = dfs[0]
#         cols = lists_cols[0]
#         leg_id = f"{trendname_or_col_prefix}_{direction.value}_leg_id"
    
#     if direction == Direction.DOWN:
#         df_ = dfs[1]
#         cols = lists_cols[1]
#         leg_id = f"{trendname_or_col_prefix}_{direction.value}_leg_id"
#         excursion()

def excursion(df: pd.DataFrame, trendname_or_col_prefix:str, leg_id_col: str, direction: Direction, mae_or_mfe: ExcursionType) -> AnnotatedDF:
    """
    Compute per-leg price excursion metrics for directional trend legs.

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
    intra_leg_idx = f'{excursion_name}_intra_leg_idx'
    df[intra_leg_idx] = df.groupby(leg_id_col).cumcount()
    announce_column_lifecycle(caller="excursion", registry=new_cols, decl=ColKeyDecl(key="intra_leg_idx", column=intra_leg_idx), event="created")    
    
    # Keep only a subset of columns
    df_slim = df.loc[:,[leg_id_col, intra_leg_idx, "open", "high", "low"]]
    announce_column_lifecycle(caller="excursion", registry=new_cols, decl=ColKeyDecl(key="MANY (FILTER)", column="List not provided"), event="dropped")

    # Get the leg length / max idx - And apply to: [all row in gorup, new col]
    leg_max_idx = f"{excursion_name}_leg_max_idx"
    leg_of_n_bars = f"{excursion_name}_leg_of_n_bars"
    df_slim[leg_max_idx] = (
        df_slim.groupby(leg_id_col)[intra_leg_idx]
        .transform("last")
    )
    df_slim[leg_of_n_bars] = df_slim[leg_max_idx] + 1
    announce_column_lifecycle(caller="excursion", registry=new_cols, decl=ColKeyDecl(key="leg_max_idx", column=leg_max_idx), event="created")
    announce_column_lifecycle(caller="excursion", registry=new_cols, decl=ColKeyDecl(key="leg_of_n_bars", column=leg_of_n_bars), event="created")

    # Get the first open - And apply to: [all rows in group, new col]
    group_first_open = f"{excursion_name}_grp_1st_open"
    df_slim[group_first_open] = (
        df_slim.groupby(leg_id_col)["open"]
        .transform("first")
    )
    announce_column_lifecycle(caller="excursion", registry=new_cols, decl=ColKeyDecl(key="group_first_open", column=group_first_open), event="created")

    # Calc Exc (also in bps)
    df_slim[f"{excursion_name}_exc"] = df_slim["high"] - df_slim[group_first_open]
    df_slim[f"{excursion_name}_exc_bps"] = delta_in_bps(df_slim[f"{excursion_name}_exc"], df_slim[group_first_open])
    
    # Mark the Exc row for each leg
    mfe_row_idx = df_slim.groupby(leg_id_col)[f"{excursion_name}_exc_bps"].idxmax()
    df_slim[f"is_{excursion_name}_row"] = False
    df_slim.loc[mfe_row_idx, f"is_{excursion_name}_row"] = True

    exc_decl = ColKeyDecl(key="excursion", column=f"{excursion_name}_exc")
    exc_bps_decl = ColKeyDecl(key="excursion_bps", column=f"{excursion_name}_exc_bps")
    is_exc_row_decl = ColKeyDecl(key="is_excursion_row", column=f"is_{excursion_name}_row")
    announce_column_lifecycle(
        caller="excursion",
        decls=[exc_decl, exc_bps_decl, is_exc_row_decl],
        event="created",
        registry=new_cols
    )

    adf = from_start_and_end(df=df_slim, prefix=excursion_name, leg_max_idx_col=leg_max_idx, intra_leg_idx_col=intra_leg_idx)
    new_cols.extend(adf.new_cols)

    adf = pct_when_excursion_max(df=adf.df, prefix=excursion_name, leg_n_bars_col=leg_of_n_bars)
    new_cols.extend(adf.new_cols)
    
    return AnnotatedDF(adf.df, new_cols)



def from_start_and_end(df: pd.DataFrame, prefix: str, leg_max_idx_col: str, intra_leg_idx_col: str) -> AnnotatedDF:
    # MFE/MAE occurs N candles from start

    _ensure_columns(df=df, cols=[leg_max_idx_col, intra_leg_idx_col], caller="pct_when_excursion_max")
    new_cols = ColRegistry()
    df[f"{prefix}_from_start"] = df[leg_max_idx_col] # no math needed
    
    # MFE/MAE occurs N candles from end
    df[f"{prefix}_from_end"] = df[leg_max_idx_col] - df[intra_leg_idx_col]
    
    announce_column_lifecycle(caller="from_start_and_end", registry=new_cols, 
        decls=[
        ColKeyDecl(key="from_start", column=f"{prefix}_from_start"), 
        ColKeyDecl(key="from_end", column=f"{prefix}_from_end"), 
        ], 
        event="created")
    
    return AnnotatedDF(df=df, new_cols=new_cols, label="from_start_and_end")
    

def pct_when_excursion_max(df: pd.DataFrame, prefix: str, leg_n_bars_col: str) -> AnnotatedDF:

    _ensure_columns(df=df, cols=leg_n_bars_col, caller="pct_when_excursion_max")

    #“What fraction of the entire realized leg had elapsed when MFE/MAE was first achieved?”
    pct_from_start_col = f"{prefix}_pct_from_start"
    df.loc[:,pct_from_start_col] = df[f"{prefix}_from_start"] / df[leg_n_bars_col]

    pct_from_end_col = f"{prefix}_pct_from_end"
    df.loc[:,pct_from_end_col] = df[f"{prefix}_from_end"] / df[leg_n_bars_col]

    sum_col = f"{prefix}_%_from_sum"
    df[sum_col] = df[pct_from_end_col] + df[pct_from_start_col]
    
    new_cols = ColRegistry()

    announce_column_lifecycle(caller="pct_when_excursion_max", registry=new_cols, 
        decls=[
            ColKeyDecl(key="idx_pct_from_start", column=pct_from_start_col), 
            ColKeyDecl(key="idx_pct_from_end", column=pct_from_end_col), 
            ColKeyDecl(key="idx_pct_sum", column=sum_col), 
        ], 
        event="created")

    return AnnotatedDF(df=df , new_cols=new_cols, label="pct_when_excursion_max")

