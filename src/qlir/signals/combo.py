from __future__ import annotations
import pandas as _pd

__all__ = ["with_combo_signal"]


def with_combo_signal(
    df: _pd.DataFrame,
    *,
    out_col: str = "sig_combo",
) -> _pd.DataFrame:
    """Example: VWAP rejection aligned with RSI regime and MACD hist sign."""
    out = df.copy()
    cond_long = (
        out.get("reject_up", 0).eq(1) &
        out.get("rsi_oversold", 0).eq(1) &
        out.get("macd_hist_pos", 0).eq(1)
    )
    cond_short = (
        out.get("reject_down", 0).eq(1) &
        out.get("rsi_overbought", 0).eq(1) &
        out.get("macd_hist_pos", 0).eq(0)
    )
    out[out_col] = 0
    out.loc[cond_long, out_col] = 1
    out.loc[cond_short, out_col] = -1
    out[out_col] = out[out_col].astype("int8")
    return out