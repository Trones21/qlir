import numpy as _np
import pandas as _pd

from qlir.df.granularity.distributions.bucketize.build_bucket_df import build_bucket_df
from qlir.logging.logdf import NamedDF


def bucketize_zoom_equal_width(
    s: _pd.Series,
    *,
    buckets: int = 20,
    threshold_pct: float = 0.9,
    max_depth: int = 6,
    int_buckets: bool = False,
    dropna: bool = True,
    human_friendly_fmt: bool = False,
    raw_values: bool = True,
) -> list[NamedDF]:
    """
    Progressive zooming equal-width bucketization.

    At each depth:
    - bucketize current range
    - if one bucket contains >= threshold_pct of values,
      zoom into that bucket and repeat
    
    int_buckets: Whether or not to RETURN buckets at integer intervals 
        (Bucket sizing always uses float)
        # e.g. 6.35 - 11.7 (bucket is 7-11 inclusive)
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
        edges = _np.linspace(current_min, current_max, buckets + 1)
        counts, _ = _np.histogram(current_values, bins=edges)

        df = build_bucket_df(
            lower_bounds=edges[:-1],
            upper_bounds=edges[1:],
            counts=counts,
            total=total,
            depth=depth,
            parent_bucket_id=None,
            human_friendly_fmt=True,
            raw_values=False
        )

        # e.g. 6.35 - 11.7 (bucket is 7-11 inclusive)
        if int_buckets:
            df["lower"] = _np.ceil(df["lower"])
            df["upper"] = _np.floor(df["upper"])

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
