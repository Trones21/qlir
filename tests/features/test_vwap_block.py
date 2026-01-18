import pandas as _pd
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

# this is old, i have since rewritten the vwap indicator
# def test_vwap_block_contract_columns_and_types():
#     # Arrange
#     df = _pd.DataFrame([
#         {"tz_start":"2024-01-01 00:00:00", "open":10, "high":11, "low": 9, "close":10.5, "volume":100},
#         {"tz_start":"2024-01-01 00:01:00", "open":10, "high":12, "low": 8, "close":11.0, "volume":120},
#     ])

#     # Act
#     out = with_vwap_feature_block(df, tz="UTC")

#     # Assert
#     assert EXPECTED_COLS.issubset(set(out.columns))
    
#     # sanity: vwap should live inside the bar envelope (common definition)
#     finite = out["vwap"].dropna()
#     assert (finite >= out.loc[finite.index, "low"]).all()
#     assert (finite <= out.loc[finite.index, "high"]).all()

