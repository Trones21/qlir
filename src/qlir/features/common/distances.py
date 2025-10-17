from __future__ import annotations
import pandas as pd

__all__ = ["add_zscore"]


def add_zscore(
    df: pd.DataFrame,
    *,
    col: str,
    window: int = 200,
    out_col: str | None = None,
    ddof0: bool = True,
) -> pd.DataFrame:
    out = df.copy()
    out_col = out_col or f"{col}_z"
    sd = out[col].rolling(window, min_periods=max(5, window//5)).std(ddof=0 if ddof0 else 1)
    out[out_col] = out[col] / sd.replace(0.0, pd.NA)
    return out