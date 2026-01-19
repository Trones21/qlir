import pandas as pd
from qlir import indicators
from qlir.core.ops import temporal
from qlir.logging.logdf import logdf
import qlir.df.reducers.distributions.bucketize.equal_width as buckets
from qlir.core.types.named_df import NamedDF
from afterdata.analysis_funcs.persistence import persistence_analysis_prep_down, persistence_analysis_prep_up


def frequency_analysis(df: pd.DataFrame, trendline_col: str) -> list[NamedDF]:
    '''
    # frequency analysis
    # count events by month - Bucketize (min = 0 events, max=max events in a single month, avid overlaps by aligning/summairze persists event start dt)
    # count events by week - Bucketize
    # Count Events by day Bucketize 
    '''

    adf = temporal.with_bar_direction(df, col=trendline_col)
    # with bar direction returns a tuple[str, ...], the second one is the sign column
    direction_col = adf.new_cols.get_column("sign")

    dfs: list[NamedDF] = []
    for_up, new_cols = persistence_analysis_prep_up(df, direction_col, trendline_col)
    group_id_col_up = new_cols[0]
    up_dfs = event_dists_by_tf(for_up, group_id_col_up, "Up Legs")
    dfs.extend(up_dfs)
    # logdf(up_dfs, max_rows=25)

    for_down, new_cols_d = persistence_analysis_prep_down(df, direction_col, trendline_col)
    group_id_col_dn = new_cols_d[0]
    dn_dfs = event_dists_by_tf(for_down, group_id_col_dn, "Down Legs")
    dfs.extend(dn_dfs)
    # logdf(dn_dfs, max_rows=25)

    compare_day_dists= [up_dfs[0], dn_dfs[0]]
    # logdf(compare_day_dists, max_rows=25)

    return dfs 


def event_dists_by_tf(df: pd.DataFrame, group_id_col: str, event_name:str) -> list[NamedDF]:
    
    tfs = {
        "D": f"{event_name} per day",
        "7D": f"{event_name} per week (rolling 7 day, not calendar based)",
        "30D": f"{event_name} per month (rolling 30 day, not calendar based)"
    }

    def legs_by(df, dt_symbol, group_id_col, label):
        legs_by = df.groupby(df.index.floor(dt_symbol)).agg(
            distinct=(group_id_col, "nunique"),
            first_ts=("tz_start", "first")
            )
        dists = buckets.bucketize_zoom_equal_width(legs_by["distinct"], int_buckets=True, human_friendly_fmt=True)
        if len(dists) > 1:
            raise RuntimeError("Bucketize returned multi depth buckets, this shouldnt occur for the legs by func")
        dist_legs_per = dists[0]
        return NamedDF(dist_legs_per.df, label)

    dfs: list[NamedDF] = []
    for symbol, label in tfs.items():
        dfs.append(legs_by(df, symbol, group_id_col, label))

    return dfs


