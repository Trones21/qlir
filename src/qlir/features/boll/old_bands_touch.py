from __future__ import annotations

import pandas as _pd

from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_boll_touch_squeeze_flags"]


def with_boll_touch_squeeze_flags(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    mid_col: str = "boll_mid",
    upper_col: str = "boll_upper",
    lower_col: str = "boll_lower",
    out_prefix: str = "boll_",
    squeeze_window: int = 20,
) -> _pd.DataFrame:
    out, ev = df_copy_measured(df=df, label="with_boll_touch_squeeze_flags")
    log_memory_debug(ev=ev, log=log)
    close = out[close_col]
    out[f"{out_prefix}touch_upper"] = (close >= out[upper_col]).astype("int8")
    out[f"{out_prefix}touch_lower"] = (close <= out[lower_col]).astype("int8")
    width = (out[upper_col] - out[lower_col]) / out[mid_col].abs()
    out[f"{out_prefix}width"] = width
    out[f"{out_prefix}squeeze"] = (width <= width.rolling(squeeze_window).quantile(0.2)).astype("int8")
    return out