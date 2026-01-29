from __future__ import annotations

import pandas as _pd

from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_combo_signal"]


def with_combo_signal(
    df: _pd.DataFrame,
    *,
    out_col: str = "sig_combo",
) -> _pd.DataFrame:
    """Example: VWAP rejection aligned with RSI regime and MACD hist sign."""
    
    raise NotImplementedError("Need to add ensure_columns and not use a default of 0 when checking for 0 (on macd_hist_pos)")
    out, ev = df_copy_measured(df=df, label="with_combo_signal")
    log_memory_debug(ev=ev, log=log)

    zero = _pd.Series(0, index=out.index)
    cond_long = (
        out.get("reject_up", zero).eq(1) &
        out.get("rsi_oversold", zero).eq(1) &
        out.get("macd_hist_pos", zero).eq(1)
    )
    cond_short = (
        out.get("reject_down", zero).eq(1) &
        out.get("rsi_overbought", zero).eq(1) &
        out.get("macd_hist_pos", zero).eq(0)
    )
    out[out_col] = 0
    out.loc[cond_long, out_col] = 1
    out.loc[cond_short, out_col] = -1
    out[out_col] = out[out_col].astype("int8")
    return out