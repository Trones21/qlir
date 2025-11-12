from qlir.features.vwap.block import with_vwap_feature_block
from qlir.features.rsi.block import with_rsi_feature_block
from qlir.features.macd.block import with_macd_feature_block
from qlir.signals import with_vwap_rejection_signal, with_combo_signal


def test_signals_exist(ohlcv_1m_100):
    df = with_vwap_feature_block(ohlcv_1m_100, tz="UTC")
    df = with_rsi_feature_block(df)
    df = with_macd_feature_block(df)
    out = with_vwap_rejection_signal(df)
    out = with_combo_signal(out)
    assert "sig_vwap_reject" in out
    assert "sig_combo" in out
    # signal column is int8
    assert str(out["sig_vwap_reject"].dtype) == "int8"