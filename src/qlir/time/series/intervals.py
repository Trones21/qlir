import pandas as _pd


def unexpected_interval_mask(
    ts: _pd.Series,
    expected_interval_s: int,
) -> _pd.Series:
    return ts.diff() != expected_interval_s
