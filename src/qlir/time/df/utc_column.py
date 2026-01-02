import pandas as pd


def add_utc_timestamp_col(
    df: pd.DataFrame,
    *,
    unix_col: str,
    unit: str,
    out_col: str = "utc_ts",
    copy: bool = True,
) -> pd.DataFrame:
    """
    Add a UTC timestamp column (second resolution) derived from a unix timestamp column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    unix_col : str
        Column containing unix timestamps.
    unit : {"s", "ms"}
        Unit of the unix timestamp column.
    out_col : str, default "utc_ts"
        Name of the output UTC timestamp column.
    copy : bool, default True
        Whether to return a copy of the DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with added UTC timestamp column.
    """
    if unit not in {"s", "ms"}:
        raise ValueError(f"unit must be 's' or 'ms', got {unit!r}")

    out = df.copy() if copy else df

    out[out_col] = (
        pd.to_datetime(out[unix_col], unit=unit, utc=True)
        .dt.floor("s")
    )

    return out
