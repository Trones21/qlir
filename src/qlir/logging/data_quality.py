from __future__ import annotations

import logging
from datetime import datetime, timezone
import pandas as pd

from qlir.logging.ensure import ensure_logging


def log_data_staleness(
    df: pd.DataFrame,
    *,
    ts_col: str,
    assume_sorted: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Log how stale the most recent timestamp in a DataFrame is relative to now (UTC).

    Assumptions
    -----------
    - Timestamps are UTC or UTC-compatible.
    - DataFrame is already sorted by time unless assume_sorted=False.

    This function is intentionally dumb:
    no candle math, no bucketing, no opinions.
    """
    ensure_logging()
    log = logger or logging.getLogger("qlir.data_quality")

    if df.empty:
        log.warning(
            "DataFrame is empty — no timestamps to evaluate."
        )
        return

    series = df[ts_col]

    latest_ts = series.iloc[-1] if assume_sorted else series.max()

    # Normalize to UTC-aware datetime
    if isinstance(latest_ts, (int, float)):
        latest_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
    else:
        latest_dt = pd.to_datetime(latest_ts, utc=True).to_pydatetime()

    now_utc = datetime.now(timezone.utc)
    delta = now_utc - latest_dt

    log.info(
        "Latest data timestamp lag detected | "
        f"latest_utc={latest_dt.isoformat()} | "
        f"now_utc={now_utc.isoformat()} | "
        f"lag={delta}"
    )
