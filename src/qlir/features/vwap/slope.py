from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_vwap_slope"]


def with_vwap_slope(df: _pd.DataFrame, *, vwap_col: str = "vwap", out_col: str = "vwap_slope") -> _pd.DataFrame:

    _ensure_columns(df=df, cols=vwap_col, caller="with_vwap_slope")
    
    out, ev = df_copy_measured(df=df, label="with_vwap_slope")
    log_memory_debug(ev=ev, log=log)

    out[out_col] = out[vwap_col].astype(float).diff()
    return out