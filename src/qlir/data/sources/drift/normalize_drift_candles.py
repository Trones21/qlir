# qlir/data/sources/drift/normalize.py

from __future__ import annotations

from collections import deque
from typing import Optional
import logging

import pandas as pd

log = logging.getLogger(__name__)

# ---- Drift-specific config -------------------------------------------------

# Column name candidates used by Drift responses
_TS_FIELDS = ["ts", "timestamp", "time"]
_OPEN_FIELDS = ["o", "open"]
_HIGH_FIELDS = ["h", "high"]
_LOW_FIELDS = ["l", "low"]
_CLOSE_FIELDS = ["c", "close"]
_BASE_VOLUME_FIELDS = ["vol", "volume", "base_volume"]
_QUOTE_VOLUME_FIELDS = ["quote_volume", "notional"]

# How far each resolution extends in time.
_RESOLUTION_SIZES: dict[str, str] = {
    "1": "1min",
    "1m": "1min",
    "5": "5min",
    "5m": "5min",
    "15": "15min",
    "15m": "15min",
    "60": "60min",
    "1h": "60min",
    "240": "240min",
    "4h": "240min",
    "1D": "1D",
    "1d": "1D",
}

# Do Drift timestamps represent the bar start or end?
_LABEL: str = "start"  # or "end" if that’s how Drift defines it

# Whether the last candle is “rolling” and its close/ending time
# should be treated as partial if it extends into the future.
_ROLLING_LAST_CLOSE: bool = True


# ---- Small helpers ---------------------------------------------------------

def _pick(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Return the first matching column from candidates (case-insensitive)."""
    lower = {c.lower(): c for c in df.columns}
    for k in candidates:
        c = lower.get(k.lower())
        if c is not None:
            return c
    return None


def _get_candle_size(resolution: str) -> pd.Timedelta:
    key = _RESOLUTION_SIZES.get(str(resolution))
    
    if key is None:
        raise ValueError(f"Unknown resolution {resolution!r}")
    
    off = pd.to_timedelta(key)

    # Always return scalar Timedelta
    if isinstance(off, pd.Timedelta):
        return off
    return off[0]   # e.g., if off is a TimedeltaIndex of length 1


# ---- Public API ------------------------------------------------------------

def normalize_drift_candles(
    df: pd.DataFrame,
    *,
    resolution: str,
    keep_ts_start_unix: bool = False,
    include_partial: bool = True,
) -> pd.DataFrame:
    """
    Normalize raw Drift candles into canonical OHLCV format.

    Canonical columns:
      - tz_start (UTC)
      - tz_end (UTC or NaT for partial last candle)
      - open, high, low, close
      - volume (optional, if present)
      - ts_start_unix (optional, if keep_ts_start_unix=True)

    Parameters
    ----------
    df :
        Raw candles returned by Drift.
    resolution :
        Drift resolution string (e.g. '1m', '5m', '1h', '1D').
    keep_ts_start_unix :
        If True, keep the original numeric timestamp as `ts_start_unix`.
    include_partial :
        If False, drop any candle whose tz_end is NaT (i.e. in-progress bar).
    """
    if df.empty:
        log.error("Empty dataframe passed to normalize_drift_candles")
        return df

    if len(df.columns) < 6:
        log.warning(
            "normalize_drift_candles passed df with %d columns. "
            "Need at least ~6 cols for full OHLCV; columns: %s",
            len(df.columns),
            list(df.columns),
        )

    # ---- OHLCV field remap ----
    ts_col = _pick(df, _TS_FIELDS) or "timestamp"
    o_col = _pick(df, _OPEN_FIELDS) or "open"
    h_col = _pick(df, _HIGH_FIELDS) or "high"
    l_col = _pick(df, _LOW_FIELDS) or "low"
    c_col = _pick(df, _CLOSE_FIELDS) or "close"

    v_col = _pick(df, _BASE_VOLUME_FIELDS) or _pick(df, _QUOTE_VOLUME_FIELDS)

    out = df.rename(
        columns={
            ts_col: "timestamp",
            o_col: "open",
            h_col: "high",
            l_col: "low",
            c_col: "close",
        }
    )
    if v_col:
        out = out.rename(columns={v_col: "volume"})

    if keep_ts_start_unix:
        out["ts_start_unix"] = out["timestamp"]

    # ---- Timestamp → UTC-aware ----
    ts = out["timestamp"]
    if pd.api.types.is_numeric_dtype(ts):
        # crude but effective heuristic: < 1e11 ≈ seconds, else ms
        unit = "s" if float(ts.max()) < 1e11 else "ms"
        ts = pd.to_datetime(ts, unit=unit, utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
    out["timestamp"] = ts

    # ---- Bounds from semantics ----
    candle_size = _get_candle_size(resolution)

    if _LABEL == "start":
        out["tz_start"] = out["timestamp"]
        out["tz_end"] = out["timestamp"] + candle_size # type: ignore[operator]
    else:  # "end"
        out["tz_end"] = out["timestamp"]
        out["tz_start"] = out["timestamp"] - candle_size

    # Mark partial last candle if tz_end in future (rolling)
    if _ROLLING_LAST_CLOSE:
        now = pd.Timestamp.now(tz="UTC")
        mask_future = out["tz_end"] > now
        out.loc[mask_future, "tz_end"] = pd.NaT
        # enforce: partials must have close = NaN (prevents accidental use)
        out.loc[out["tz_end"].isna(), "close"] = pd.NA

    if not include_partial:
        out = out[out["tz_end"].notna()]

    # ---- Final canonical order ----
    cols = deque(["tz_start", "tz_end", "open", "high", "low", "close"])
    if "volume" in out:
        cols.append("volume")
    if keep_ts_start_unix:
        cols.appendleft("ts_start_unix")

    out = out[list(cols)].reset_index(drop=True)
    return out
