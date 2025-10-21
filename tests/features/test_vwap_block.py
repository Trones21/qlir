import pandas as pd
from qlir.features.vwap.block import add_vwap_feature_block


# tests/features/test_vwap_block.py
import pandas as pd
from pandas.api.types import is_datetime64tz_dtype, is_integer_dtype
from qlir.features.vwap.block import add_vwap_feature_block

def bars(rows):
    """
    rows = list of dicts with keys: ts, o,h,l,c,v
    Returns a normalized OHLCV DataFrame (tz-aware UTC).
    """
    df = pd.DataFrame([{
        "timestamp": r["ts"],
        "open": r["o"], "high": r["h"], "low": r["l"], "close": r["c"],
        "volume": r["v"],
    } for r in rows])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


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
    df = bars([
        {"ts":"2024-01-01T00:00:00Z", "o":10, "h":11, "l": 9, "c":10.5, "v":100},
        {"ts":"2024-01-01T00:01:00Z", "o":10, "h":12, "l": 8, "c":11.0, "v":120},
    ])

    # Act
    out = add_vwap_feature_block(df, tz="UTC")

    # Assert
    assert EXPECTED_COLS.issubset(set(out.columns))
    assert is_datetime64tz_dtype(out["timestamp"])

    for c in FLAG_COLS:
        assert is_integer_dtype(out[c]), f"{c} must be integer-like (0/1)"

    # must be monotonic time, no duplicates
    assert out["timestamp"].is_monotonic_increasing
    assert not out["timestamp"].duplicated().any()

    # sanity: vwap should live inside the bar envelope (common definition)
    finite = out["vwap"].dropna()
    assert (finite >= out.loc[finite.index, "low"]).all()
    assert (finite <= out.loc[finite.index, "high"]).all()














# def test_vwap_block_contract(ohlcv_1m_100):
#     out = add_vwap_feature_block(ohlcv_1m_100, tz="UTC")
#     expected = {"vwap", "relation", "cross_up", "cross_down", "reject_up", "reject_down", "session", "streak_len", "vwap_dist", "vwap_z"}
#     assert expected.issubset(set(out.columns))
#     # dtypes on flags
#     for c in ["cross_up", "cross_down", "reject_up", "reject_down"]:
#         assert str(out[c].dtype) == "int8"


# def test_vwap_block_dst(ohlcv_dst_boundary):
#     df, tz = ohlcv_dst_boundary
#     out = add_vwap_feature_block(df, tz=tz)
#     # session should split exactly at local midnight boundaries despite DST fallback
#     assert "session" in out
#     # There should be at least two distinct session values if the range crosses midnight
#     assert out["session"].nunique() >= 1