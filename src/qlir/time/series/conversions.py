import pandas as pd


def unix_to_utc_series(
    s: pd.Series,
    unit: str,
) -> pd.Series:
    return pd.to_datetime(s, unit=unit, utc=True).dt.floor("s")
