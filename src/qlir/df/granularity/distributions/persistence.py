import pandas as _pd

from qlir.core.types.named_df import NamedDF
import qlir.df.granularity.distributions.bucketize.lossy.equal_width as buckets

def condition_persistence(df: _pd.DataFrame, group_id_col: str, persistence_col: str) -> list[NamedDF]:
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