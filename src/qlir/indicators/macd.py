from __future__ import annotations
import pandas as _pd

__all__ = ["with_macd"]


def with_macd(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    out_macd: str = "macd",
    out_signal: str = "macd_signal",
    out_hist: str = "macd_hist",
    in_place: bool = True,
) -> _pd.DataFrame:
    out = df if in_place else df.copy()
    close = out[close_col].astype(float)
    
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    
    out["ema_fast"] = ema_fast
    out["ema_slow"] = ema_slow
    
    out[out_macd] = macd
    out[out_signal] = sig
    out[out_hist] = macd - sig

    # MACD line only valid once we have enough data 
    out["macd_line_ready"] = df.index >= (slow - 1)
    # Signal line & Histogram need even more time
    out["macd_signal_line_and_hist_ready"] = df.index >= (slow + signal - 1)

    return out