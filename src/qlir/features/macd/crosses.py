from __future__ import annotations
import pandas as _pd

__all__ = ["with_macd_cross_flags"]


def with_macd_cross_flags(
    df: _pd.DataFrame,
    *,
    macd_col: str = "macd",
    signal_col: str = "macd_signal",
) -> _pd.DataFrame:
    out = df.copy()
    diff = out[macd_col] - out[signal_col]
    prev = diff.shift(1)
    out["macd_cross_up"] = ((diff > 0) & (prev <= 0)).astype("int8")
    out["macd_cross_down"] = ((diff < 0) & (prev >= 0)).astype("int8")
    out["macd_hist_pos"] = (diff > 0).astype("int8")
    return out