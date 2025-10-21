from qlir.indicators.macd import add_macd


def test_macd_columns(ohlcv_1m_100):
    out = add_macd(ohlcv_1m_100)
    for c in ["macd", "macd_signal", "macd_hist"]:
        assert c in out.columns