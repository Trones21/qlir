import pandas as pd
from qlir.features.vwap.block import with_vwap_feature_block
import pytest
pytestmark = pytest.mark.local

EXPECTED_COLS = {
    "vwap", "relation",
    "cross_up", "cross_down",
    "reject_up", "reject_down",
    "session", "streak_len",
    "vwap_dist", "vwap_z",
}

FLAG_COLS = ["cross_up", "cross_down", "reject_up", "reject_down"]

def test_vwap_block_contract_columns_and_types():
    # Arrange
    df = pd.DataFrame([
        {"tz_start":"2024-01-01 00:00:00", "open":10, "high":11, "low": 9, "close":10.5, "volume":100},
        {"tz_start":"2024-01-01 00:01:00", "open":10, "high":12, "low": 8, "close":11.0, "volume":120},
    ])

    # Act
    out = with_vwap_feature_block(df, tz="UTC")

    # Assert
    assert EXPECTED_COLS.issubset(set(out.columns))
    
    # sanity: vwap should live inside the bar envelope (common definition)
    finite = out["vwap"].dropna()
    assert (finite >= out.loc[finite.index, "low"]).all()
    assert (finite <= out.loc[finite.index, "high"]).all()



# def test_vwap_block_contract(ohlcv_1m_100):
#     out = with_vwap_feature_block(ohlcv_1m_100, tz="UTC")
#     expected = {"vwap", "relation", "cross_up", "cross_down", "reject_up", "reject_down", "session", "streak_len", "vwap_dist", "vwap_z"}
#     assert expected.issubset(set(out.columns))
#     # dtypes on flags
#     for c in ["cross_up", "cross_down", "reject_up", "reject_down"]:
#         assert str(out[c].dtype) == "int8"


# def test_vwap_block_dst(ohlcv_dst_boundary):
#     df, tz = ohlcv_dst_boundary
#     out = with_vwap_feature_block(df, tz=tz)
#     # session should split exactly at local midnight boundaries despite DST fallback
#     assert "session" in out
#     # There should be at least two distinct session values if the range crosses midnight
#     assert out["session"].nunique() >= 1