import pandas as _pd


def unix_to_utc_series(
    s: _pd.Series,
    unit: str,
) -> _pd.Series:
    return _pd.to_datetime(s, unit=unit, utc=True).dt.floor("s")
