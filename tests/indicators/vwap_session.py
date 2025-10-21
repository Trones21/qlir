import numpy as np
import pandas as pd
from qlir.indicators.vwap import add_vwap_hlc3_session

def test_vwap_hlc3_resets_each_session_minimal():
    # 3 bars on day 1 at ~10, 2 bars on day 2 at ~20
    ts = pd.date_range("2025-01-01 23:58:00Z", periods=5, freq="1min")
    df = pd.DataFrame(
        {
            "open":  [10, 10, 10, 20, 20],
            "high":  [10, 11, 12, 20, 30],
            "low":   [10, 11, 12, 20, 30],
            "close": [10, 11, 12, 20, 30],
            "volume":[ 1,  1,  1,  1,  1],
        },
        index=ts,
    )
    df = df.rename_axis("timestamp").reset_index()
    out = add_vwap_hlc3_session(df, tz="UTC")  # this one uses time utils
    print(out)
    v = out["vwap"]
    assert np.allclose(v.iloc[:3], 10.0)
    assert np.allclose(v.iloc[3:], 20.0)



def test_vwap_session_midnight_end_semantics():
    ts = pd.to_datetime([
        "2025-01-01 23:59:00Z",  # day 1
        "2025-01-02 00:00:00Z",  # should be day 2 under 'end' semantics
    ])
    df = pd.DataFrame(
        {
            "open":  [10, 20],
            "high":  [10, 20],
            "low":   [10, 20],
            "close": [10, 20],
            "volume":[ 1,  1],
        },
        index=ts,
    )
    out = add_vwap_hlc3_session(df, tz="UTC")
    v = out["vwap"].to_numpy()

    # independent ground truth
    # day1 vwap = 10; day2 vwap = 20 (each is a single bar)
    np.testing.assert_allclose(v, [10.0, 20.0], atol=1e-12)

