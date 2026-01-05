# reducers/distributions/bucketize/shared.py

from __future__ import annotations

import pandas as _pd
import numpy as _np


def build_bucket_df(
    *,
    lower_bounds: _np.ndarray,
    upper_bounds: _np.ndarray,
    counts: _np.ndarray,
    total: int,
    depth: int = 0,
    parent_bucket_id: int | None = None,
) -> _pd.DataFrame:
    """
    Canonical bucket manifest → DataFrame
    """
    df = _pd.DataFrame({
        "lower": lower_bounds,
        "upper": upper_bounds,
        "count": counts,
    })

    df["pct"] = df["count"] / total
    df["depth"] = depth
    df["parent_bucket_id"] = parent_bucket_id
    df["bucket_id"] = range(len(df))

    return df[
        [
            "bucket_id",
            "lower",
            "upper",
            "count",
            "pct",
            "depth",
            "parent_bucket_id",
        ]
    ]
