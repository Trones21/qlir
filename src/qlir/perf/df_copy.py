import os
import time
from typing import Optional
import psutil
import pandas as pd

from qlir.perf.memory_event import MemoryEvent

_process = psutil.Process(os.getpid())

def df_copy_measured(
    df: pd.DataFrame,
    *,
    deep: bool = True,
    label: Optional[str] = None,
):
    # ---- before ----
    rss_before = _process.memory_info().rss
    df_bytes_before = df.memory_usage(deep=True).sum()

    t0 = time.perf_counter()
    df2 = df.copy(deep=deep)
    elapsed = time.perf_counter() - t0

    # ---- after ----
    rss_after = _process.memory_info().rss
    df_bytes_after = df2.memory_usage(deep=True).sum()

    event = MemoryEvent(
        label=label,
        df_bytes_before=df_bytes_before,
        df_bytes_after=df_bytes_after,
        rss_before=rss_before,
        rss_after=rss_after,
        elapsed_s=elapsed,
    )

    return df2, event

