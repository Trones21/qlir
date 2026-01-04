import numpy as np
import pandas as pd
from qlir.df.reducers.distributions.bucketize.build_bucket_df import build_bucket_df
from qlir.utils.logdf import NamedDF


def bucketize_zoom_equal_width(
    s: pd.Series,
    *,
    buckets: int = 20,
    threshold_pct: float = 0.9,
    max_depth: int = 6,
    dropna: bool = True,
) -> list[NamedDF]:
    """
    Progressive zooming equal-width bucketization.

    At each depth:
    - bucketize current range
    - if one bucket contains >= threshold_pct of values,
      zoom into that bucket and repeat
    """
    if dropna:
        s = s.dropna()

    values = s.to_numpy()
    total = len(values)

    if total == 0:
        return []

    out: list[NamedDF] = []

    current_values = values
    current_min = values.min()
    current_max = values.max()

    for depth in range(max_depth):
        # --- bucketize current range ---
        edges = np.linspace(current_min, current_max, buckets + 1)
        counts, _ = np.histogram(current_values, bins=edges)

        df = build_bucket_df(
            lower_bounds=edges[:-1],
            upper_bounds=edges[1:],
            counts=counts,
            total=total,
            depth=depth,
            parent_bucket_id=None,
        )

        out.append(NamedDF(df, f"zoom:depth{depth}"))

        # --- find densest bucket ---
        max_idx = counts.argmax()
        max_pct = counts[max_idx] / total

        # stop if distribution is informative enough
        if max_pct < threshold_pct:
            break

        # --- zoom into that bucket ---
        lower = edges[max_idx]
        upper = edges[max_idx + 1]

        mask = (values >= lower) & (values < upper)
        current_values = values[mask]

        # if zooming doesn’t reduce range meaningfully, stop
        if lower == current_min and upper == current_max:
            break

        current_min = lower
        current_max = upper

    return out
