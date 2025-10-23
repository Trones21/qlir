import logging
from qlir.indicators.vwap import add_vwap_cum_hlc3
from qlir.features.common.distances import add_distance
from qlir.utils.logdf import logdf
import numpy as np
import pandas as pd


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

def distance_ref(close, vwap):
    out = []
    for x, m in zip(close, vwap):
        out.append(np.nan if (m is None or m == 0 or x is None) else (x - m) / m)
    return pd.Series(out, name="pct")

def test_distance_against_reference(static_data):
    # Arrange - Get vwap since we are going to use that 
    df = add_vwap_cum_hlc3(static_data).head(50)

    # Act
    got = add_distance(df, from_="vwap", to_="close")
    
    # An implementation we know is correct
    ref = distance_ref(df["close"].tolist(), df["vwap"].tolist())

    got_series = got["vwap_to_close_pct"].reset_index(drop=True)
    ref_series = ref.reset_index(drop=True)

    try:
        pd.testing.assert_series_equal(
            got_series, ref_series,
            rtol=1e-10, atol=1e-12, check_names=False
        )
    except AssertionError:
        diff = pd.DataFrame({
            "got": got_series,
            "ref": ref_series,
            "diff": got_series - ref_series,
        })
        logdf(diff, 100, name="Mismatch Detected")
        raise
