import pandas as pd


def unexpected_interval_mask(
    ts: pd.Series,
    expected_interval_s: int,
) -> pd.Series:
    return ts.diff() != expected_interval_s
