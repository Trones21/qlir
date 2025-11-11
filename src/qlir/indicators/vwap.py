# src/qlir/indicators/vwap.py
from __future__ import annotations
import numpy as np
import pandas as pd
from ..time.misc import session_floor
from ..utils.df_ops import ensure_copy

__all__ = ["add_vwap_cum_hlc3", "add_vwap_hlc3_session"]

def add_vwap_cum_hlc3(
    df: pd.DataFrame,
    *,
    price_cols=("high", "low", "close"),
    volume_col="volume",
    out_col="vwap",
    in_place=False,
) -> pd.DataFrame:
    out = df if in_place else df.copy()
    h, l, c = price_cols
    vol = out[volume_col].astype(float)
    hlc3 = (out[h] + out[l] + out[c]) / 3.0

    cum_vol = vol.cumsum()
    out[out_col] = (hlc3 * vol).cumsum() / cum_vol
    out.loc[cum_vol == 0, out_col] = np.nan
    return out



# indicators/vwap.py
def add_vwap_hlc3_session(df, *, tz="UTC", price_cols=("high","low","close"), volume_col="volume", out_col="vwap"):
    out = df.copy()
    h, l, c = price_cols
    vol = out[volume_col].astype(float)
    hlc3 = ((out[h] + out[l] + out[c]) / 3.0).astype(float)
    
    # Session = calendar day of ts_end in tz (index is ts_end, thanks to data layer)
    if isinstance(out.index, pd.DatetimeIndex):
        session = out.index.tz_convert(tz).floor("D")
    else:
        # Fallback if someone passes columns instead of index
        session = pd.to_datetime(out["ts_end"], utc=True).dt.tz_convert(tz).dt.floor("D")

    cumv = vol.groupby(session).cumsum()
    vwap = (hlc3 * vol).groupby(session).cumsum() / cumv
    vwap[cumv == 0] = float("nan")
    out[out_col] = vwap
    return out

