from __future__ import annotations
import pandas as pd

__all__ = ["add_distance_metrics"]


def add_distance_metrics(
    df: pd.DataFrame,
    *,
    vwap_col: str = "vwap",
    price_col: str = "close",
    out_prefix: str = "vwap_",
    norm_window: int | None = 200,
    use_pop_std: bool = True,
) -> pd.DataFrame:
    out = df.copy()
    dist = out[price_col] - out[vwap_col]
    out[f"{out_prefix}dist"] = dist
    out[f"{out_prefix}dist_pct"] = dist / out[vwap_col] * 100.0
    out[f"{out_prefix}dist_abs"] = dist.abs()
    out[f"{out_prefix}avg_dist"] = out[f"{out_prefix}dist_abs"].expanding().mean()
    out[f"{out_prefix}max_dist"] = out[f"{out_prefix}dist_abs"].expanding().max()
    if norm_window:
        sd = dist.rolling(norm_window, min_periods=max(5, norm_window//5)).std(ddof=0 if use_pop_std else 1)
        out[f"{out_prefix}z"] = dist / sd.replace(0.0, pd.NA)
    return out