from __future__ import annotations
import numpy as np
import pandas as pd
from ..utils.time import session_floor
from ..utils.df_ops import ensure_copy

__all__ = ["add_vwap_session"]


def add_vwap_session(
    df: pd.DataFrame,
    *,
    tz: str = "UTC",
    price_cols: tuple[str, str, str] = ("high", "low", "close"),
    volume_col: str = "volume",
    out_col: str = "vwap",
    in_place: bool = False,
) -> pd.DataFrame:
    """Session-reset VWAP (resets at local midnight in `tz`). Requires tz-aware `timestamp`."""
    out = ensure_copy(df, in_place)
    h, l, c = price_cols
    vol = out[volume_col].astype(float)
    tp = ((out[h] + out[l] + out[c]) / 3.0).astype(float)

    session = session_floor(out, tz=tz)
    out["_session"] = session

    pv = tp * vol
    g = out.groupby("_session", sort=False)
    pv_cum = g[ pv.name ].cumsum()
    v_cum  = g[ vol.name ].cumsum()

    denom = v_cum.replace(0.0, np.nan)
    out[out_col] = (pv_cum / denom).to_numpy()
    return out