from __future__ import annotations
import pandas as pd
from ...utils.time import session_floor

__all__ = ["add_session_id"]


def add_session_id(df: pd.DataFrame, *, tz: str = "UTC", ts_col: str = "timestamp", out_col: str = "session") -> pd.DataFrame:
    out = df.copy()
    out[out_col] = session_floor(out, tz=tz, ts_col=ts_col).astype("category")
    return out