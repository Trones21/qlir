from __future__ import annotations
import pandas as pd

__all__ = ["ensure_tzaware", "session_floor"]


def ensure_tzaware(df: pd.DataFrame, ts_col: str = "timestamp") -> None:
    ts = df[ts_col]
    if not isinstance(ts.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{ts_col} must be tz-aware (got {ts.dtype})")


def session_floor(df: pd.DataFrame, tz: str = "UTC", ts_col: str = "timestamp") -> pd.Series:
    """DST-safe local midnight, preserving tz dtype."""
    ensure_tzaware(df, ts_col)
    return df[ts_col].dt.tz_convert(tz).dt.floor("D")