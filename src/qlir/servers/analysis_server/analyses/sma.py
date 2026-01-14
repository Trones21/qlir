import pandas as _pd
from qlir import indicators
from qlir.core.ops import temporal
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.analyses.df_prep.persistence import persistence_analysis_prep_down, persistence_analysis_prep_up
import logging
log = logging.getLogger(__name__)

def sma(clean_data: _pd.DataFrame, direction_persistence: int) -> tuple[_pd.DataFrame | None, bool]:

    df, sma_cols = indicators.sma(clean_data, col="open", window=direction_persistence, decimals=6)
    logdf(df)
    sma_col = sma_cols[0] #type: ignore (fix later, lol, works but pylance issue (decorator))
    if not isinstance(sma_col, str):
        return None , True
    df[sma_col].round(6)

    df, cols = temporal.with_bar_direction(clean_data, col=sma_col)
    
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    direction = cols["sign"]

    for_up_df, up_cols = persistence_analysis_prep_up(df, direction, sma_col)
    for_down_df, down_cols = persistence_analysis_prep_down(df, direction, sma_col)

    # Just check the base columns and rows are all the same
    shared = for_up_df.columns.intersection(for_down_df.columns)
    log.info(f"up_df: {len(for_up_df)} down_df: {len(for_down_df)}")
    if len(for_up_df) != len(for_down_df):
        raise AssertionError("Up and down dfs shoul be of the same length")

    df_out = for_up_df.combine_first(for_down_df)

    return (df_out, False)