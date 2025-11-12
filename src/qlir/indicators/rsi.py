from __future__ import annotations
import pandas as pd
import numpy as np

__all__ = ["with_rsi"]


def with_rsi(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 14,
    out_col: str = "rsi",
    in_place: bool = False,
) -> pd.DataFrame:
    out = df if in_place else df.copy()
    close = out[close_col].astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # Wilder's smoothing via EMA with alpha=1/period
    roll_up = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    roll_dn = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs = roll_up / roll_dn.replace(0.0, np.nan)
    out[out_col] = 100 - (100 / (1 + rs))
    return out