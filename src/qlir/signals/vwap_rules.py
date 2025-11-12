from __future__ import annotations
import pandas as pd

__all__ = ["with_vwap_rejection_signal"]


def with_vwap_rejection_signal(
    df: pd.DataFrame,
    *,
    need_slope_same_side: bool = True,
    out_col: str = "sig_vwap_reject",
) -> pd.DataFrame:
    out = df.copy()
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