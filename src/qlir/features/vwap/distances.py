from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_distance_metrics"]


def with_distance_metrics(
    df: _pd.DataFrame,
    *,
    vwap_col: str = "vwap",
    price_col: str = "close",
    out_prefix: str = "vwap_",
    norm_window: int | None = 200,
    use_pop_std: bool = True,
) -> _pd.DataFrame:
    
    _ensure_columns(df=df, cols=[price_col, vwap_col], caller="with_distance_metrics")

    out, ev = df_copy_measured(df=df, label="with_distance_metrics")
    log_memory_debug(ev=ev, log=log)
    
    dist: _pd.Series = out[price_col] - out[vwap_col]
    out[f"{out_prefix}dist"] = dist
    out[f"{out_prefix}dist_pct"] = dist / out[vwap_col] * 100.0
    out[f"{out_prefix}dist_abs"] = dist.abs()
    out[f"{out_prefix}avg_dist"] = out[f"{out_prefix}dist_abs"].expanding().mean()
    out[f"{out_prefix}max_dist"] = out[f"{out_prefix}dist_abs"].expanding().max()

    # Relationship to price (string)
    out.loc[dist > 0,  "price_rel_vwap"] = 'above'
    out.loc[dist < 0, "price_rel_vwap"] = 'below'
    out["price_rel_vwap"].fillna("equal", inplace=True)

    if norm_window:
        sd = dist.rolling(norm_window, min_periods=max(5, norm_window//5)).std(ddof=0 if use_pop_std else 1)
        out[f"{out_prefix}z"] = dist / sd.replace(0.0, _pd.NA)
        
    return out