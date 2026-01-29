from __future__ import annotations

import numpy as _np
import pandas as _pd

from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.df.utils import _ensure_columns
from qlir.core.semantics.events import log_column_event
from qlir.core.registries.columns.lifecycle import ColumnLifecycleEvent
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["flag_relations"]


def flag_relations(
    df: _pd.DataFrame,
    *,
    vwap_col: str = "vwap",
    price_col: str = "close",
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    touch_eps: float = 5e-4,     # 5 bps
    touch_min_abs: float = 0.0,  # e.g., $0.01
    out_rel: str = "relation",
) -> _pd.DataFrame:
    _ensure_columns(df=df, cols=[vwap_col, price_col, *ohlc], caller="flag_relations")
    out, ev = df_copy_measured(df=df, label="flag_relations")
    log_memory_debug(ev=ev, log=log)

    diff = out[price_col] - out[vwap_col]
    tol = (out[vwap_col].abs() * touch_eps).fillna(0.0)
    if touch_min_abs > 0:
        tol = _np.maximum(tol, touch_min_abs)

    rel = _np.where(diff.abs() <= tol, "touch", _np.where(diff > 0, "above", "below"))
    out[out_rel] = _pd.Categorical(rel, categories=["below", "touch", "above"], ordered=True)

    prev_rel = out[out_rel].shift(1)
    out["cross_up"] = (out[out_rel].eq("above") & prev_rel.eq("below")).astype("int8")
    out["cross_down"] = (out[out_rel].eq("below") & prev_rel.eq("above")).astype("int8")

    out["reject_down"] = ((out[ohlc.high] >= out[vwap_col]) & (out[price_col] < out[vwap_col])).astype("int8")
    out["reject_up"]   = ((out[ohlc.low]  <= out[vwap_col]) & (out[price_col] > out[vwap_col])).astype("int8")

    log_column_event(caller="flag_relations", ev=ColumnLifecycleEvent(col="cross_up", event="created"))
    log_column_event(caller="flag_relations", ev=ColumnLifecycleEvent(col="cross_down", event="created"))
    log_column_event(caller="flag_relations", ev=ColumnLifecycleEvent(col="reject_up", event="created"))
    log_column_event(caller="flag_relations", ev=ColumnLifecycleEvent(col="reject_down", event="created"))

    return out