from __future__ import annotations
import pandas as _pd
import numpy as _np

from qlir.df.utils import _ensure_columns

__all__ = ["rsi"]


def rsi(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 14,
    out_col: str = "rsi",
    in_place: bool = True,
) -> _pd.DataFrame:
    
    _ensure_columns(df=df, cols=close_col, caller="rsi")
    
    out = df if in_place else df.copy()
    close = out[close_col].astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # Wilder's smoothing via EMA with alpha=1/period
    roll_up = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    roll_dn = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs = roll_up / roll_dn.replace(0.0, _np.nan)
    out[out_col] = 100 - (100 / (1 + rs))
    return out