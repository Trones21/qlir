from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

from ...time.misc import session_floor

import logging
log = logging.getLogger(__name__)

__all__ = ["with_session_id"]


def with_session_id(df: _pd.DataFrame, *, tz: str = "UTC", ts_col: str = "tz_start", out_col: str = "session") -> _pd.DataFrame:
    _ensure_columns(df=df, cols=ts_col, caller="with_session_id")

    out, ev = df_copy_measured(df=df, label="with_session_id")
    log_memory_debug(ev=ev, log=log)
    
    out[out_col] = session_floor(out, tz=tz, ts_col=ts_col).astype("category")
    return out