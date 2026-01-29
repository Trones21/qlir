from __future__ import annotations

import pandas as _pd

from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_vwap_rejection_signal"]


def with_vwap_rejection_signal(
    df: _pd.DataFrame,
    *,
    need_slope_same_side: bool = True,
    out_col: str = "sig_vwap_reject",
) -> _pd.DataFrame:
    out, ev = df_copy_measured(df=df, label="with_vwap_rejection_signal")
    log_memory_debug(ev=ev, log=log)

    
    long_cond = out.get("reject_up", 0).eq(1)
    short_cond = out.get("reject_down", 0).eq(1)
    if need_slope_same_side and "vwap_slope" in out.columns:
        long_cond &= out["vwap_slope"] >= 0
        short_cond &= out["vwap_slope"] <= 0
    out[out_col] = 0
    out.loc[long_cond, out_col] = 1
    out.loc[short_cond, out_col] = -1
    out[out_col] = out[out_col].astype("int8")
    return out