from __future__ import annotations
import pandas as pd

__all__ = ["add_macd"]


def add_macd(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    out_macd: str = "macd",
    out_signal: str = "macd_signal",
    out_hist: str = "macd_hist",
    in_place: bool = False,
) -> pd.DataFrame:
    out = df if in_place else df.copy()
    close = out[close_col].astype(float)
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    out[out_macd] = macd
    out[out_signal] = sig
    out[out_hist] = macd - sig
    return out