from __future__ import annotations
import pandas as _pd

from qlir.df.utils import _ensure_columns
from ...time.misc import session_floor

__all__ = ["with_session_id"]


def with_session_id(df: _pd.DataFrame, *, tz: str = "UTC", ts_col: str = "tz_start", out_col: str = "session") -> _pd.DataFrame:
    _ensure_columns(df=df, cols=ts_col, caller="with_session_id")
    out = df.copy()
    out[out_col] = session_floor(out, tz=tz, ts_col=ts_col).astype("category")
    return out