from qlir.features.vwap.block import add_vwap_feature_block
from qlir.features.rsi.block import add_rsi_feature_block
from qlir.features.macd.block import add_macd_feature_block
from qlir.signals import add_vwap_rejection_signal, add_combo_signal


def test_signals_exist(ohlcv_1m_100):
    df = add_vwap_feature_block(ohlcv_1m_100, tz="UTC")
    df = add_rsi_feature_block(df)
    df = add_macd_feature_block(df)
    out = add_vwap_rejection_signal(df)
    out = add_combo_signal(out)
    assert "sig_vwap_reject" in out
    assert "sig_combo" in out
    # signal column is int8
    assert str(out["sig_vwap_reject"].dtype) == "int8"