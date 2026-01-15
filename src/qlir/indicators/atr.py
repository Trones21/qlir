# qlir/indicators/volatility.py  (or wherever you keep these)
from __future__ import annotations

from typing import Optional

import pandas as _pd

from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.df.utils import _ensure_columns

try:
    import talib
except ImportError:
    talib = None


def with_atr(
    df: _pd.DataFrame,
    *,
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
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

    _ensure_columns(df=df, cols=[*ohlc], caller="atr")

    if out_col is None:
        out_col = f"atr_{period}"

    atr_vals = talib.ATR(
        df[ohlc.high].values,
        df[ohlc.low].values,
        df[ohlc.close].values,
        timeperiod=period,
    )

    # attach to same df (your DF-in → DF-out pattern)
    df[out_col] = atr_vals
    return df
