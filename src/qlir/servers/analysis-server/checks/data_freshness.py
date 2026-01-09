# analysis_server/checks/data_freshness.py

from datetime import datetime, timezone, timedelta
import pandas as pd


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def data_lag(
    data_ts: pd.Timestamp,
) -> timedelta:
    """
    Compute lag between now and the latest data timestamp.
    """
    return utc_now() - data_ts.to_pydatetime()


def is_data_stale(
    data_ts: pd.Timestamp,
    *,
    max_lag: timedelta,
) -> bool:
    """
    Return True if data is older than max_lag.
    """
    return data_lag(data_ts) > max_lag
