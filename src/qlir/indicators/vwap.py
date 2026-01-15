# src/qlir/indicators/vwap.py
from __future__ import annotations

import numpy as _np
import pandas as _pd

from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.df.utils import _ensure_columns

__all__ = ["with_vwap_cum_hlc3", "with_vwap_hlc3_session"]

def with_vwap_cum_hlc3(
    df: _pd.DataFrame,
    *,
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    volume_col="volume",
    out_col="vwap",
    in_place=False,
) -> _pd.DataFrame:
    
    _ensure_columns(df, [*ohlc], caller="vwap_cum_hlc3")

    out = df if in_place else df.copy()
    vol = out[volume_col].astype(float)
    hlc3 = (out[ohlc.high] + out[ohlc.low] + out[ohlc.close]) / 3.0

    cum_vol = vol.cumsum()
    out[out_col] = (hlc3 * vol).cumsum() / cum_vol
    out.loc[cum_vol == 0, out_col] = _np.nan
    return out



# indicators/vwap.py
def with_vwap_hlc3_session(df, *, tz="UTC", ohlc: OHLC_Cols = DEFAULT_OHLC_COLS, volume_col="volume", out_col="vwap"):


    _ensure_columns(df, [*ohlc], caller='vwap_hlc3_session')
    
    out = df.copy()
    vol = out[volume_col].astype(float)
    hlc3 = ((out[ohlc.high] + out[ohlc.low] + out[ohlc.close]) / 3.0).astype(float)
    
    # Session = calendar day of ts_end in tz (index is ts_end, thanks to data layer)
    if isinstance(out.index, _pd.DatetimeIndex):
        session = out.index.tz_convert(tz).floor("D")
    else:
        # Fallback if someone passes columns instead of index
        session = _pd.to_datetime(out["ts_end"], utc=True).dt.tz_convert(tz).dt.floor("D")

    cumv = vol.groupby(session).cumsum()
    vwap = (hlc3 * vol).groupby(session).cumsum() / cumv
    vwap[cumv == 0] = float("nan")
    out[out_col] = vwap
    return out

