from __future__ import annotations

import pandas as pd

from .utils import ensure_utc, DEFAULT_TS_COL


def add_calendar_labels(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    *,
    year: bool = True,
    quarter: bool = True,
    month: bool = True,
    dow: bool = True,
    hour: bool = True,
    minute: bool = False,
) -> pd.DataFrame:
    """
    Add common calendar/intraday columns to the df.
    """
    df = ensure_utc(df, col)
    out = df.copy()
    dt = out[col]
    if year:
        out["year"] = dt.dt.year
    if quarter:
        out["quarter"] = ((dt.dt.month - 1) // 3) + 1
    if month:
        out["month"] = dt.dt.month
    if dow:
        out["dow"] = dt.dt.dayofweek
    if hour:
        out["hour"] = dt.dt.hour
    if minute:
        out["minute"] = dt.dt.minute
    return out
