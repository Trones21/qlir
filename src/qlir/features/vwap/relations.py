from __future__ import annotations
import numpy as _np
import pandas as _pd

__all__ = ["flag_relations"]


def flag_relations(
    df: _pd.DataFrame,
    *,
    vwap_col: str = "vwap",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    touch_eps: float = 5e-4,     # 5 bps
    touch_min_abs: float = 0.0,  # e.g., $0.01
    out_rel: str = "relation",
) -> _pd.DataFrame:
    out = df.copy()
    diff = out[price_col] - out[vwap_col]
    tol = (out[vwap_col].abs() * touch_eps).fillna(0.0)
    if touch_min_abs > 0:
        tol = _np.maximum(tol, touch_min_abs)

    rel = _np.where(diff.abs() <= tol, "touch", _np.where(diff > 0, "above", "below"))
    out[out_rel] = _pd.Categorical(rel, categories=["below", "touch", "above"], ordered=True)

    prev_rel = out[out_rel].shift(1)
    out["cross_up"] = (out[out_rel].eq("above") & prev_rel.eq("below")).astype("int8")
    out["cross_down"] = (out[out_rel].eq("below") & prev_rel.eq("above")).astype("int8")

    out["reject_down"] = ((out[high_col] >= out[vwap_col]) & (out[price_col] < out[vwap_col])).astype("int8")
    out["reject_up"]   = ((out[low_col]  <= out[vwap_col]) & (out[price_col] > out[vwap_col])).astype("int8")
    return out