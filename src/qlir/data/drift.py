from __future__ import annotations
import math
from typing import Literal, Optional, Dict, Any
import pandas as pd
import requests

DRIFT_BASE = "https://data.api.drift.trade"

Resolution = Literal["1", "5", "15", "60", "240", "D", "W", "M"]

# Field alias maps we’ve seen from Drift deployments
CANDLE_ALIASES = {
    "timestamp": ["ts", "timestamp", "time"],
    "open":      ["open", "fillOpen"],
    "high":      ["high", "fillHigh"],
    "low":       ["low", "fillLow"],
    "close":     ["close", "fillClose"],
    # Prefer base volume (asset units). Quote volume (e.g., USDC) is optional.
    "baseVolume":  ["baseVolume", "volume", "vol"],
    "quoteVolume": ["quoteVolume", "notionalVolume"],
}

def _rename_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c.lower(): c for c in df.columns}
    def pick(keys: list[str]) -> Optional[str]:
        return next((lower[k.lower()] for k in keys if k.lower() in lower), None)

    colmap: Dict[str, str] = {}
    # Required
    for target, options in [("timestamp", CANDLE_ALIASES["timestamp"]),
                            ("open", CANDLE_ALIASES["open"]),
                            ("high", CANDLE_ALIASES["high"]),
                            ("low", CANDLE_ALIASES["low"]),
                            ("close", CANDLE_ALIASES["close"])]:
        src = pick(options)
        if not src:
            raise ValueError(f"Missing required field for '{target}' (looked for {options})")
        colmap[src] = target

    # Volume: prefer baseVolume; fallback to quoteVolume if that’s all we have.
    base_src = pick(CANDLE_ALIASES["baseVolume"])
    quote_src = pick(CANDLE_ALIASES["quoteVolume"])
    if base_src:
        colmap[base_src] = "volume"
    elif quote_src:
        colmap[quote_src] = "volume"
    else:
        # allow empty; caller can fill later if needed
        pass

    out = df.rename(columns=colmap)
    # Coerce types
    if "timestamp" in out:
        # ts often arrives as seconds; if it’s numeric and looks like seconds, convert with unit='s'
        if pd.api.types.is_numeric_dtype(out["timestamp"]):
            # crude heuristic: any values < 10^11 are probably seconds, not ms
            unit = "s" if float(out["timestamp"].max()) < 1e11 else "ms"
            out["timestamp"] = pd.to_datetime(out["timestamp"], unit=unit, utc=True, errors="coerce")
        else:
            out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        if col in out:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp")
    return out

def fetch_drift_candles(
    symbol: str = "SOL-PERP",
    resolution: Resolution = "1",
    limit: Optional[int] = None,
    start_time: Optional[int] = None,  # epoch seconds
    end_time: Optional[int] = None,    # epoch seconds
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
) -> pd.DataFrame:
    """
    Fetch candles from Drift Data API and normalize to:
    columns = ['timestamp','open','high','low','close','volume']
    Times are UTC. Volume is base asset units when available.
    """
    sess = session or requests.Session()
    url = f"{DRIFT_BASE}/market/{symbol}/candles/{resolution}"
    params: Dict[str, Any] = {}
    if limit: params["limit"] = int(limit)
    if start_time: params["startTime"] = int(start_time)
    if end_time: params["endTime"] = int(end_time)

    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()

    # The playground wraps arrays under 'records' in many endpoints.
    records = payload.get("records", payload)
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df = _rename_to_canonical(df)
    # keep only canonical columns if present
    keep = [c for c in ["timestamp","open","high","low","close","volume"] if c in df.columns]
    return df[keep]
