from __future__ import annotations
import pandas as pd

__all__ = ["ensure_tzaware", "session_floor"]


def ensure_tzaware(df: pd.DataFrame, ts_col: str = "timestamp") -> None:
    ts = df[ts_col]
    if not isinstance(ts.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{ts_col} must be tz-aware (got {ts.dtype})")


# src/qlir/utils/time.py
import pandas as pd

def session_floor(df, tz="UTC", ts_col: str | None = "timestamp", *, bar_semantics: str = "end"):
    """
    Return per-row session keys (local calendar day).
    bar_semantics:
      - "end": timestamp marks period end (default). Bar at 00:00 -> NEW day.
      - "start": timestamp marks period start. Bar at 00:00 -> prior day.
    """
    # get timestamp series (column if present, else index)
    if ts_col and ts_col in df.columns:
        ts = pd.to_datetime(df[ts_col], utc=True)
    else:
        ts = pd.to_datetime(df.index, utc=True)

    # Convert to local tz
    local = ts.dt.tz_convert(tz)

    if bar_semantics == "start":
        # Shift to end-of-bar for session bucketing (needs bar freq)
        raise NotImplementedError("Provide bar freq if you need 'start' semantics.")
        # e.g., local = local + pd.Timedelta(minutes=1)

    # For 'end' semantics we do nothing special:
    # bars at exactly 00:00 belong to the new calendar day.
    return local.dt.floor("D")
