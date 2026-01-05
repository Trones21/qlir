import numpy as _np
import pandas as _pd
from typing import Iterable
import logging
logg = logging.getLogger(__name__)

def series_angle(
    df: _pd.DataFrame,
    *,
    cols: Iterable[str],
    window: int,
    prefix: str | None = None,
    log: bool = True,
    degrees: bool = True,
    in_place: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Compute geometric angle of a series over a window.

    Defined as the angle of the secant line between t-window and t:
        angle = atan((x[t] - x[t-window]) / window)

    If log=True, computation is done in log-space (scale invariant).
    """

    if window <= 0:
        raise ValueError("window must be > 0")

    out = df if in_place else df.copy()
    new_cols: list[str] = []

    for col in cols:
        base = f"{prefix + '_' if prefix else ''}{col}"
        name = f"{base}_w{window}_angle"

        s = _np.log(out[col]) if log else out[col]
        slope = (s - s.shift(window)) / window
        angle = _np.arctan(slope)

        if degrees:
            logg.info("Using degrees")
            angle = _np.degrees(angle)
            name += "_deg"

        out[name] = angle
        new_cols.append(name)

    return out, tuple(new_cols)
