# data/normalize.py
from typing import Optional
import pandas as pd
from .registry import CandleSpec, REGISTRY

def _pick(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for k in candidates:
        c = lower.get(k.lower())
        if c is not None:
            return c
    return None

def _offset(spec: CandleSpec, resolution: str) -> pd.Timedelta:
    key = spec.res_to_offset.get(str(resolution))
    if not key:
        raise ValueError(f"Unknown resolution {resolution!r} for venue {spec.venue}")
    return pd.to_timedelta(key)

def normalize_candles(
    df: pd.DataFrame,
    *,
    venue: str,
    resolution: str,
    include_partial: bool = True,
) -> pd.DataFrame:
    """
    Returns canonical columns:
      tz_start (UTC), tz_end (UTC or NaT for partial), open, high, low, close, volume
    - Applies venue-specific timestamp semantics + OHLCV field choices.
    - If include_partial=False ⇒ last in-progress bar removed (tz_end > now).
    """
    spec = REGISTRY[venue]
    if df.empty:
        return df

    # ---- OHLCV field remap ----
    ts_col = _pick(df, spec.timestamp_fields) or "timestamp"
    o_col  = _pick(df, spec.open_fields)      or "open"
    h_col  = _pick(df, spec.high_fields)      or "high"
    l_col  = _pick(df, spec.low_fields)       or "low"
    c_col  = _pick(df, spec.close_fields)     or "close"
    v_col  = _pick(df, spec.base_volume_fields) or _pick(df, spec.quote_volume_fields)

    out = df.rename(columns={ts_col: "timestamp", o_col: "open", h_col: "high", l_col: "low", c_col: "close"})
    if v_col:
        out = out.rename(columns={v_col: "volume"})

    # Timestamp → UTC-aware
    ts = out["timestamp"]
    if pd.api.types.is_numeric_dtype(ts):
        unit = "s" if float(ts.max()) < 1e11 else "ms"
        ts = pd.to_datetime(ts, unit=unit, utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
    out["timestamp"] = ts

    # Keep needed columns
    keep = ["timestamp", "open", "high", "low", "close"] + (["volume"] if "volume" in out.columns else [])
    out = out[keep].dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp").reset_index(drop=True)

    # ---- Bounds from semantics ----
    off = _offset(spec, resolution)
    if spec.label == "start":
        out["tz_start"] = out["timestamp"]
        out["tz_end"] = out["timestamp"] + off
    else:  # "end"
        out["tz_end"] = out["timestamp"]
        out["tz_start"] = out["timestamp"] - off

    # Mark partial last candle if tz_end in future (rolling)
    if spec.rolling_last_close:
        now = pd.Timestamp.now(tz="UTC")
        out.loc[out["tz_end"] > now, "tz_end"] = pd.NaT
        # enforce: partials must have close = NaN (prevents accidental use)
        out.loc[out["tz_end"].isna(), "close"] = pd.NA

    if not include_partial:
        out = out[out["tz_end"].notna()]

    # Final canonical order
    cols = ["tz_start", "tz_end", "open", "high", "low", "close"]
    if "volume" in out:
        cols.append("volume")
    out = out[cols].reset_index(drop=True)
    return out
