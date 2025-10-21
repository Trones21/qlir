from qlir.indicators.boll import add_bollinger


def test_boll_cols(ohlcv_1m_100):
    out = add_bollinger(ohlcv_1m_100)
    for c in ["boll_mid", "boll_upper", "boll_lower"]:
        assert c in out.columns
