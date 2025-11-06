from __future__ import annotations

import pandas as pd

DEFAULT_TS_COL = "tz_start"


def ensure_utc(df: pd.DataFrame, col: str = DEFAULT_TS_COL) -> pd.DataFrame:
    """
    Ensure df[col] is datetime64[ns, UTC]. Returns a (possibly) copied df.
    """
    if not pd.api.types.is_datetime64_any_dtype(df[col]):
        df = df.copy()
        df[col] = pd.to_datetime(df[col], utc=True)
    else:
        if df[col].dt.tz is None:
            df = df.copy()
            df[col] = df[col].dt.tz_localize("UTC")
    return df
