import numpy as np
import pandas as pd
from trading.indicators.vwap import add_vwap_session
from trading.features.vwap.relations import flag_relations


def test_touch_eps_threshold():
    # Build two bars where close is nearly equal to vwap within tolerance
    idx = pd.date_range("2025-01-01", periods=2, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": idx,
        "open": [100, 100],
        "high": [100, 100],
        "low": [100, 100],
        "close": [100, 100.01],
        "volume": [1000, 1000],
    })
    df = add_vwap_session(df, tz="UTC")
    out = flag_relations(df, touch_eps=5e-4, touch_min_abs=0.02)  # 2 cents min abs
    # Since diff is 1 cent, with min abs 2 cents, second bar should be 'touch'
    assert out.loc[1, "relation"] == "touch"