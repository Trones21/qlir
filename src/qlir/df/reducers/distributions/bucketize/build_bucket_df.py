# reducers/distributions/bucketize/shared.py

from __future__ import annotations

import pandas as pd
import numpy as np


def build_bucket_df(
    *,
    lower_bounds: np.ndarray,
    upper_bounds: np.ndarray,
    counts: np.ndarray,
    total: int,
    depth: int = 0,
    parent_bucket_id: int | None = None,
) -> pd.DataFrame:
    """
    Canonical bucket manifest → DataFrame
    """
    df = pd.DataFrame({
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
