


import pandas as pd
from qlir.core.ops import temporal
from qlir.core.types.named_df import NamedDF
from qlir.df.granularity.distributions.persistence import condition_persistence
from qlir.column_bundles.persistence import persistence_down_legs, persistence_up_legs
import logging
log = logging.getLogger(__name__)

def persistence_analysis(df: pd.DataFrame, trendline_col: str) -> list[NamedDF]:

    df, cols = temporal.with_bar_direction(df, col=trendline_col)
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    log.info(cols)
    direction_col = cols['sign']

    dfs: list[NamedDF] = []

    for_up_pers_df, _ = persistence_up_legs(df, direction_col, trendline_col)
    up_legs_dists = condition_persistence(for_up_pers_df, f"{trendline_col}_up_leg_id", "up_leg_run_len")
    up_legs_dist = up_legs_dists[0]
    up_legs_dist.name = f"{trendline_col}_up_leg_run_len"
    dfs.append(up_legs_dist)
    # logdf(up_legs_dists, max_rows=25)

    for_down_pers_df, _ = persistence_down_legs(df, direction_col, trendline_col)
    down_legs_dists = condition_persistence(for_down_pers_df, f"{trendline_col}_down_leg_id", "down_leg_run_len")
    down_legs_dist = down_legs_dists[0]
    down_legs_dist.name = f"{trendline_col}_down_leg_run_len"
    dfs.append(down_legs_dist)
    # logdf(down_legs_dists, max_rows=25)

    return dfs