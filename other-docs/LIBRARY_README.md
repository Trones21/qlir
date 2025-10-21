## Architecture notes 

Data is normalized before written to flat files, currently just calling normalize at the end of fetch, for example:

```python
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
```

the last line here is important, we are using a registry to provide information about the specific API. For example:
- Fieldnames provided
- Timestamp Logic (if not tz_start and tz_end), does this timestamp represent the open or the close?
- Current Bar: If we are retreiving the current bar, then is close included? (uses tick data? smaller timeframe data? etc.) 

## Run cli 


```sh
# See Options 

# Get candle data and write to csv 
make run ARGS="fetch --out ./example.csv"
```





```python
import pandas as pd
from trading.api import add_vwap_feature_block, add_rsi_feature_block
from trading.signals import add_vwap_rejection_signal, add_combo_signal

# df must contain tz-aware `timestamp`, and OHLCV columns
out = add_vwap_feature_block(df, tz="America/Los_Angeles")
out = add_rsi_feature_block(out)
out = add_vwap_rejection_signal(out)
out = add_combo_signal(out)
```

---

## Quick test scaffolding (pytest idea)

```python
# tests/test_vwap_block.py
import pandas as pd
import numpy as np
from trading.features.vwap.block import add_vwap_feature_block

def test_runs():
    idx = pd.date_range("2025-01-01", periods=100, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": idx,
        "open": np.linspace(100, 101, len(idx)),
        "high": np.linspace(100.5, 101.5, len(idx)),
        "low":  np.linspace(99.5, 100.5, len(idx)),
        "close": np.linspace(100, 101, len(idx)),
        "volume": 1000,
    })
    out = add_vwap_feature_block(df, tz="UTC")
    assert "vwap" in out and "relation" in out
```

---

### Notes
- All orchestrators are intentionally thin; extend them freely (more flags, thresholds, etc.).
- Column names are parametric everywhere, so you can adapt to Drift/your feeds.
- Keep cross-indicator features in `features/combo/` (add later) to avoid circular deps.
- All indicator feature folders have a `block.py`, the purpose of this is to give you a quick way to add all the features of a given indicator to your dataframe.

For Example:

```python
def add_vwap_feature_block(
    df: pd.DataFrame,
    *,
    tz: str = "UTC",
    touch_eps: float = 5e-4,
    norm_window: int | None = 200,
) -> pd.DataFrame:
    out = add_vwap_session(df, tz=tz)
    out = flag_relations(out, touch_eps=touch_eps)
    out = add_session_id(out, tz=tz)
    out = add_counts_running(out, session_col="session", rel_col="relation")
    out = add_streaks(out, session_col="session", rel_col="relation")
    out = add_vwap_slope(out)
    out = add_distance_metrics(out, norm_window=norm_window)
    return out
```

This is also a good place to copy from.

--- 

Notes for Author

- Ensure that new features are added to the feature block as they are written. This is not a huge deal, but definitely a nice to have