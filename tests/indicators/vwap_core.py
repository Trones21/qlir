# tests/indicators/test_vwap_core.py
import numpy as np
import pandas as pd
import pandas.testing as pdt
from qlir.indicators.vwap import add_vwap_cum_hlc3
import pytest
pytestmark = pytest.mark.local

def test_vwap_hlc3_basic_no_time():
    # Simple RangeIndex data (no timestamps, no tz)
    df = pd.DataFrame({
        "high":  [10, 11, 12, 13],
        "low":   [ 9, 10, 11, 12],
        "close": [10, 10, 11, 12],
        "volume":[ 1,  2,  3,  4],
    })
    out = add_vwap_cum_hlc3(df)
    # Ground truth
    hlc3 = (df.high + df.low + df.close) / 3.0
    cum_vol = df.volume.cumsum()
    exp = (hlc3 * df.volume).cumsum() / cum_vol
    pdt.assert_series_equal(out, exp, check_names=False)

def test_vwap_hlc3_zero_volume_no_time():
    df = pd.DataFrame({
        "high":  [10, 11, 12, 13, 14],
        "low":   [ 9, 10, 11, 12, 13],
        "close": [10, 10, 11, 12, 13],
        "volume":[ 0,  2,  0,  3,  4],   # zeros sprinkled in
    })
    out = add_vwap_cum_hlc3(df)
    hlc3 = (df.high + df.low + df.close)/3.0
    cum_vol = df.volume.astype(float).cumsum()
    exp = (hlc3 * df.volume).cumsum() / cum_vol
    exp[cum_vol == 0] = np.nan
    pdt.assert_series_equal(out, exp, check_names=False)

    # carry-over: when row vol==0 and cumvol>0, VWAP stays the same
    v = out.to_numpy()
    vol = df.volume.to_numpy()
    for i in range(1, len(df)):
        if vol[i] == 0 and not np.isnan(v[i-1]):
            assert np.isclose(v[i], v[i-1])
