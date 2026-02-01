

from typing import Optional, Sequence

import pandas as pd

from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_info
import logging
log = logging.getLogger(__name__)

def union_with_granularity(
    frames: Sequence[tuple[int, pd.DataFrame]],
    *,
    granularity_col: str,
    granularity_label_col: Optional[str] = "granularity",
    copy: bool = False,
) -> pd.DataFrame:
    """
    frames: [(n_minutes, df), ...]
    Adds granularity columns and concatenates row-wise.

    granularity_min: int (e.g. 1, 2, 5, 15)
    granularity: optional string label (e.g. "1m", "2m", "15m")
    """
    pieces: list[pd.DataFrame] = []

    for n, df in frames:
        
        if copy: 
            part, ev = df_copy_measured(df=df, label="apply_fill_policy")
            log_memory_info(ev=ev, log=log)
        else:
            part = df

        part[granularity_col] = int(n)
        if granularity_label_col is not None:
            part[granularity_label_col] = f"{int(n)}m"
        pieces.append(part)

    # ignore_index=False keeps the DatetimeIndex (great for Tableau exports too)
    out = pd.concat(pieces, axis=0, ignore_index=False)

    # Optional: stable sorting so Tableau extracts look sane
    out = out.sort_index()

    return out
