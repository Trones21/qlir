from __future__ import annotations
import pandas as pd
from ...time.misc import session_floor

__all__ = ["with_session_id"]


def with_session_id(df: pd.DataFrame, *, tz: str = "UTC", ts_col: str = "timestamp", out_col: str = "session") -> pd.DataFrame:
    out = df.copy()
    out[out_col] = session_floor(out, tz=tz, ts_col=ts_col).astype("category")
    return out