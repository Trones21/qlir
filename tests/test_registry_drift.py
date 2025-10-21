from qlir.data import normalize_candles
import pandas as pd 

def test_registry_drift_daily_norm_marks_partial():
    raw = pd.DataFrame({
        "timestamp": [pd.Timestamp("2025-10-19T00:00Z"), pd.Timestamp("2025-10-20T00:00Z")],
        "open":[1,2], "high":[1,2], "low":[1,2], "close":[1.1, 2.2], "volume":[10, 20],
    })
    out = normalize_candles(raw, venue="drift", resolution="D", include_partial=True)
    assert out.loc[0, "tz_end"] == pd.Timestamp("2025-10-20T00:00Z")
    assert pd.isna(out.loc[1, "tz_end"])            # last is partial
    assert pd.isna(out.loc[1, "close"])             # partial close masked
