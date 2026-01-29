from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_macd_cross_flags"]


def with_macd_cross_flags(
    df: _pd.DataFrame,
    *,
    macd_col: str = "macd",
    signal_col: str = "macd_signal",
) -> _pd.DataFrame:
    _ensure_columns(df=df, cols=[macd_col, signal_col], caller="with_macd_cross_flags")
    
    out, ev = df_copy_measured(df=df, label="with_macd_cross_flags")
    log_memory_debug(ev=ev, log=log)
    
    diff = out[macd_col] - out[signal_col]
    prev = diff.shift(1)
    out["macd_cross_up"] = ((diff > 0) & (prev <= 0)).astype("int8")
    out["macd_cross_down"] = ((diff < 0) & (prev >= 0)).astype("int8")
    out["macd_hist_pos"] = (diff > 0).astype("int8")
    return out