# src/qlir/indicators/vwap.py
from __future__ import annotations

import numpy as _np
import pandas as _pd

from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.semantics.events import log_column_event
from qlir.core.semantics.row_derivation import ColumnLifecycleEvent
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.df.utils import _ensure_columns

__all__ = ["with_vwap_cum_hlc3", "with_vwap_hlc3_grouped", "with_vwap_hlc3_session"]


def with_vwap_hlc3_grouped(
    df: _pd.DataFrame,
    *,
    groupby,
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    volume_col: str = "volume",
    out_col: str = "vwap",
    in_place: bool = False,
) -> _pd.DataFrame:
    """
    Compute HLC3-based VWAP, resetting cumulatives per group.

    Parameters
    ----------
    groupby
        Any valid pandas groupby key:
        - column name
        - list of column names
        - Series aligned to df.index
        - callable(df) -> Series
    """

    _ensure_columns(df, [*ohlc, volume_col], caller="with_vwap_hlc3_grouped")

    out = df if in_place else df.copy()

    vol = out[volume_col].astype(float)
    hlc3 = (out[ohlc.high] + out[ohlc.low] + out[ohlc.close]) / 3.0

    # Resolve group key
    if callable(groupby):
        group_key = groupby(out)
    else:
        group_key = groupby

    cum_vol = vol.groupby(group_key).cumsum()
    cum_pv = (hlc3 * vol).groupby(group_key).cumsum()

    out[out_col] = cum_pv / cum_vol
    out.loc[cum_vol == 0, out_col] = _np.nan

    log_column_event(
        caller="with_vwap_hlc3_grouped",
        ev=ColumnLifecycleEvent(col=out_col, event="created"),
    )

    return out


def with_vwap_cum_hlc3(
    df: _pd.DataFrame,
    *,
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    volume_col: str = "volume",
    out_col: str = "vwap",
    in_place: bool = False,
) -> _pd.DataFrame:
    return with_vwap_hlc3_grouped(
        df,
        groupby=lambda _: 0,  # single global group
        ohlc=ohlc,
        volume_col=volume_col,
        out_col=out_col,
        in_place=in_place,
    )


def with_vwap_hlc3_session(
    df: _pd.DataFrame,
    *,
    tz: str = "UTC",
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    volume_col: str = "volume",
    out_col: str = "vwap",
    in_place: bool = False,
) -> _pd.DataFrame:

    def session_key(df_: _pd.DataFrame):
        if isinstance(df_.index, _pd.DatetimeIndex):
            return df_.index.tz_convert(tz).floor("D")
        return (
            _pd.to_datetime(df_["ts_end"], utc=True)
            .dt.tz_convert(tz)
            .dt.floor("D")
        )

    return with_vwap_hlc3_grouped(
        df,
        groupby=session_key,
        ohlc=ohlc,
        volume_col=volume_col,
        out_col=out_col,
        in_place=in_place,
    )