# qlir/data/sources/drift/normalize.py

from __future__ import annotations

from collections import deque
from typing import Optional
import logging
from qlir.utils.logdf import logdf
import pandas as pd

log = logging.getLogger(__name__)

# ---- Drift-specific config -------------------------------------------------
_QUOTE_VOLUME = 'quoteVolume'
_BASE_VOLUME = 'baseVolume'
_FILLS_OHLC_COLS = ['fillOpen', 'fillHigh', 'fillLow', 'fillClose']
_ORACLE_OHLC_COLS = ['oracleOpen', 'oracleHigh', 'oracleLow', 'oracleClose']

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
    "D": "1D"
}

# Do Drift timestamps represent the bar start or end?
_LABEL: str = "start"  # or "end" if that’s how Drift defines it

# Whether the last candle is “rolling” and its close/ending time
# should be treated as partial if it extends into the future.
_ROLLING_LAST_CLOSE: bool = True


# ---- Small helpers ---------------------------------------------------------

def _get_candle_size(resolution: str) -> pd.Timedelta:
    key = _RESOLUTION_SIZES.get(str(resolution))
    
    if key is None:
        raise ValueError(f"Unknown resolution {resolution!r}")
    
    off = pd.to_timedelta(key)

    # Always return scalar Timedelta
    if isinstance(off, pd.Timedelta):
        return off
    return off[0]   # e.g., if off is a TimedeltaIndex of length 1

def rename_by_index(df: pd.DataFrame, old_cols, new_cols):
    if len(old_cols) != len(new_cols):
        raise ValueError("old_cols and new_cols must be same length")
    return df.rename(columns=dict(zip(old_cols, new_cols)))

# ---- Public API ------------------------------------------------------------

def normalize_drift_candles(
    df: pd.DataFrame,
    *,
    resolution: str,
    keep_oracle: bool,
    keep_fills: bool,
    keep_ts_start_unix: bool = False,
    include_partial: bool = True,
) -> pd.DataFrame:
    """
    Normalize raw Drift candles into canonical OHLCV format.

    Canonical columns:
      - tz_start (UTC)
      - tz_end (UTC or NaT for partial last candle)
      - open, high, low, close (or not renamed if both oracle and fills are kept)
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


    out = df

    if keep_ts_start_unix:
        out["ts_start_unix"] = out["ts"]

    # ---- Timestamp → UTC-aware ----
    ts = out["ts"]
    if pd.api.types.is_numeric_dtype(ts):
        # crude but effective heuristic: < 1e11 ≈ seconds, else ms
        unit = "s" if float(ts.max()) < 1e11 else "ms"
        ts = pd.to_datetime(ts, unit=unit, utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
    out["ts"] = ts

    # ---- Bounds from semantics ----
    candle_size = _get_candle_size(resolution)

    if _LABEL == "start":
        out["tz_start"] = out["ts"]
        out["tz_end"] = out["ts"] + candle_size # type: ignore[operator]
    else:  # "end"
        out["tz_end"] = out["ts"]
        out["tz_start"] = out["ts"] - candle_size

    # Mark partial last candle if tz_end in future (rolling)
    # if _ROLLING_LAST_CLOSE:
    #     now = pd.Timestamp.now(tz="UTC")
    #     mask_future = out["tz_end"] > now
    #     out.loc[mask_future, "tz_end"] = pd.NaT
    #     # enforce: partials must have close = NaN (prevents accidental use)
    #     out.loc[out["tz_end"].isna(), "close"] = pd.NA

    if not include_partial:
        out = out[out["tz_end"].notna()]

   
    cols = deque(["tz_start", "tz_end"])
    if keep_ts_start_unix:
        cols.appendleft("ts_start_unix") 
    
    if keep_oracle and keep_fills:
        # we are keeping both fills and oracle so we arent doing any renaming
        cols.extend(_FILLS_OHLC_COLS)
        cols.extend(_ORACLE_OHLC_COLS)
    else:
        ohlc_canonical = ['open', 'high', 'low', 'close']
        # rename oracle or fills ohlc cols to canonical ohlc
        if keep_oracle:
            out = rename_by_index(out, _ORACLE_OHLC_COLS, ohlc_canonical)
            cols.extend(ohlc_canonical)
        if keep_fills:
            out = rename_by_index(out, _FILLS_OHLC_COLS, ohlc_canonical)
            cols.extend(ohlc_canonical)

    # this will filter out any unwanted cols 
    out = out[list(cols)].reset_index(drop=True)
    return out
