import numpy as _np
from qlir.indicators.rsi import with_rsi


def test_rsi_sane_range(ohlcv_1m_100):
    out = with_rsi(ohlcv_1m_100, period=14)
    rsi = out["rsi"].dropna()
    assert ((rsi >= 0) & (rsi <= 100)).all()