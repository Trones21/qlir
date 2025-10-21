# data/drift.py
import requests, pandas as pd
from typing import Any, Dict, Optional
from .normalize import normalize_candles

DRIFT_BASE = "https://data.api.drift.trade"

def fetch_drift_candles(
    symbol: str = "SOL-PERP",
    resolution: str = "1",
    limit: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
    *,
    include_partial: bool = True,
) -> pd.DataFrame:
    sess = session or requests.Session()
    url = f"{DRIFT_BASE}/market/{symbol}/candles/{resolution}"
    params: Dict[str, Any] = {}
    if limit: params["limit"] = int(limit)
    if start_time: params["startTime"] = int(start_time)
    if end_time: params["endTime"] = int(end_time)

    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    records = payload.get("records", payload)
    df = pd.DataFrame(records)
    if df.empty:
        return df

    # one line: push through venue spec
    return normalize_candles(df, venue="drift", resolution=resolution, include_partial=include_partial)
