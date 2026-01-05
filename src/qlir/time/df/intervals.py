import pandas as _pd
from qlir.time.series.intervals import unexpected_interval_mask

def flag_unexpected_intervals_df(
    df: _pd.DataFrame,
    ts_col: str,
    expected_interval_s: int,
    out_col: str = "unexpected_interval",
) -> _pd.DataFrame:
    df = df.copy()
    df[out_col] = unexpected_interval_mask(df[ts_col], expected_interval_s)
    return df
