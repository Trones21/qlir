import numpy as np
from qlir.indicators.rsi import add_rsi


def test_rsi_sane_range(ohlcv_1m_100):
    out = add_rsi(ohlcv_1m_100, period=14)
    rsi = out["rsi"].dropna()
    assert ((rsi >= 0) & (rsi <= 100)).all()