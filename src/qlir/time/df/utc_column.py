import pandas as _pd

from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

def add_utc_timestamp_col(
    df: _pd.DataFrame,
    *,
    unix_col: str,
    unit: str,
    out_col: str = "utc_ts",
    copy: bool = True,
) -> _pd.DataFrame:
    """
    Add a UTC timestamp column (second resolution) derived from a unix timestamp column.

    Parameters
    ----------
    df : _pd.DataFrame
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
    _pd.DataFrame
        DataFrame with added UTC timestamp column.
    """
    if unit not in {"s", "ms"}:
        raise ValueError(f"unit must be 's' or 'ms', got {unit!r}")

    if copy:
        out, ev = df_copy_measured(df=df, label="add_utc_timestamp_col")
        log_memory_debug(ev=ev, log=log)
    else:
        out = df

    out[out_col] = (
        _pd.to_datetime(out[unix_col], unit=unit, utc=True)
        .dt.floor("s")
    )

    return out
