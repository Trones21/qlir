# qlir/indicators/volatility.py  (or wherever you keep these)
from __future__ import annotations
from typing import Optional

import pandas as _pd

try:
    import talib
except ImportError:
    talib = None


def with_atr(
    df: _pd.DataFrame,
    *,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    period: int = 14,
    out_col: Optional[str] = None,
) -> _pd.DataFrame:
    """
    Return a DataFrame with a TA-Lib ATR column appended.

    Parameters
    ----------
    df : _pd.DataFrame
        Must contain high/low/close columns.
    high_col, low_col, close_col : str
        Column names for the OHLC data.
    period : int, default 14
        ATR period passed to TA-Lib.
    out_col : str, optional
        Name of the output column. Defaults to "atr_{period}".

    Returns
    -------
    _pd.DataFrame
        Same DataFrame object with the ATR column added.

    Raises
    ------
    RuntimeError
        If TA-Lib is not installed.
    """
    if talib is None:
        raise RuntimeError("TA-Lib is required for with_atr(); install ta-lib + talib-python.")

    if out_col is None:
        out_col = f"atr_{period}"

    atr_vals = talib.ATR(
        df[high_col].values,
        df[low_col].values,
        df[close_col].values,
        timeperiod=period,
    )

    # attach to same df (your DF-in → DF-out pattern)
    df[out_col] = atr_vals
    return df
