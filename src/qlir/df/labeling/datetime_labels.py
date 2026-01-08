from __future__ import annotations

import pandas as _pd

from qlir.df.utils import _ensure_columns
from qlir.time.constants import DEFAULT_TS_COL
from qlir.time.ensure_utc import ensure_utc_df_strict


def add_calendar_labels(
    df: _pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    *,
    year: bool = True,
    quarter: bool = True,
    month: bool = True,
    dow: bool = True,
    hour: bool = True,
    minute: bool = False,
) -> _pd.DataFrame:
    """
    Add common calendar/intraday columns to the df.
    """
    _ensure_columns(df=df, cols=col, caller="add_calendar_labels")
    df = ensure_utc_df_strict(df, col)
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
