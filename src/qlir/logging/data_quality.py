from __future__ import annotations

from datetime import datetime, timezone
import logging

import pandas as pd

from qlir.logging.ensure import ensure_logging


def log_data_staleness(
    df: pd.DataFrame,
    *,
    assume_sorted: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Log how stale the most recent timestamp in a DataFrame is relative to now (UTC).

    Requirements
    ------------
    - df.index must be a tz-aware DatetimeIndex.
    """
    ensure_logging()
    log = logger or logging.getLogger("qlir.data_quality")

    if df.empty:
        log.warning("DataFrame is empty — no timestamps to evaluate.")
        return

    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("log_data_staleness requires a DatetimeIndex")

    if df.index.tz is None:
        raise ValueError("DatetimeIndex must be timezone-aware")

    latest = df.index[-1] if assume_sorted else df.index.max()

    # Normalize (cheap + explicit)
    latest_dt = latest.tz_convert("UTC").to_pydatetime()

    now_utc = datetime.now(timezone.utc)
    delta = now_utc - latest_dt

    log.info(
        "Latest data timestamp lag detected | "
        f"latest_utc={latest_dt.isoformat()} | "
        f"now_utc={now_utc.isoformat()} | "
        f"lag={delta}"
    )
