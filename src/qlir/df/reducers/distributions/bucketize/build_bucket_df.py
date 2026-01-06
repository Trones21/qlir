# reducers/distributions/bucketize/shared.py

from __future__ import annotations

import pandas as _pd
import numpy as _np
from qlir.df.utils import move_column

def build_bucket_df(
    *,
    lower_bounds: _np.ndarray,
    upper_bounds: _np.ndarray,
    counts: _np.ndarray,
    total: int,
    depth: int = 0,
    human_friendly_fmt: bool = False,
    raw_values: bool = True,
    parent_bucket_id: int | None = None,
) -> _pd.DataFrame:
    """
    bucket → DataFrame
    """

    df = _pd.DataFrame({
        "lower": lower_bounds,
        "upper": upper_bounds,
        "count": counts,
    })

    df["pct"] = df["count"] / total
    df["cum_pct"] = df["pct"].cumsum()
    df["depth"] = depth
    df["parent_bucket_id"] = parent_bucket_id
    df["bucket_id"] = range(len(df))
    
    if human_friendly_fmt:
        df["pct_fmt"] = (df["pct"] * 100).map("{:.2f}%".format)
        df["cum_pct_fmt"] = (df["cum_pct"] * 100).map("{:.2f}%".format)

    if not raw_values:
        df.drop(columns=["pct", "cum_pct"], inplace=True)

    # Send bucket id to front 
    df = move_column(df, "bucket_id", 0)

    # # Send depth and parent bucket id to back
    df = move_column(df, "depth", -1)
    # df = move_column(df, "parent_bucket_id", -1)

    return df