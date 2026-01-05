# tests/indicators/test_with_atr_math.py
import pytest
import pandas as _pd
import numpy as _np

from qlir.indicators.atr import with_atr

# we still need talib because with_atr uses it right now
pytest.importorskip("talib")


def _make_small_ohlc() -> _pd.DataFrame:
    # Chosen so the math stays clean
    # idx: 0      1     2     3     4
    high  = [10.0, 11.0, 12.0, 11.5, 12.2]
    low   = [ 9.0,  9.5, 10.5, 10.8, 11.7]
    close = [ 9.5, 10.5, 11.0, 11.2, 12.0]
    idx = _pd.date_range("2025-01-01", periods=5, freq="D")
    return _pd.DataFrame({"high": high, "low": low, "close": close}, index=idx)


def _manual_wilder_atr(df: _pd.DataFrame, period: int) -> _pd.Series:
    """
    Manual ATR so we can assert on actual math.

    Steps:
    - TR_0 = high_0 - low_0
    - TR_t = max(
        high_t - low_t,
        abs(high_t - close_{t-1}),
        abs(low_t  - close_{t-1})
      )
    - ATR_{period-1} = average(TR_0 ... TR_{period-1})
    - ATR_t = (ATR_{t-1} * (period - 1) + TR_t) / period
    """
    h = df["high"]
    l = df["low"]
    c = df["close"]

    tr = []
    for i in range(len(df)):
        if i == 0:
            tr.append(h[i] - l[i])
        else:
            tr_i = max(
                h[i] - l[i],
                abs(h[i] - c[i - 1]),
                abs(l[i] - c[i - 1]),
            )
            tr.append(tr_i)

    tr = _pd.Series(tr, index=df.index, name="tr")

    atr_vals = [_np.nan] * len(df)

    # seed ATR at index = period-1
    seed_idx = period - 1
    seed_atr = tr.iloc[:period].mean()
    atr_vals[seed_idx] = seed_atr

    # recursive Wilder smoothing
    for i in range(seed_idx + 1, len(df)):
        prev_atr = atr_vals[i - 1]
        atr_vals[i] = (prev_atr * (period - 1) + tr.iloc[i]) / period

    return _pd.Series(atr_vals, index=df.index, name=f"atr_{period}")


def test_with_atr_matches_manual_wilder_math():
    df = _make_small_ohlc()
    period = 3

    # what we expect, computed explicitly
    expected = _manual_wilder_atr(df, period=period)

    # run the real function (currently TA-Lib under the hood)
    out = with_atr(df.copy(), period=period)  # should create "atr_3"
    got = out[f"atr_{period}"]

    # same index, same length
    assert list(got.index) == list(expected.index)

    # compare values (allow NaN at the top)
    _np.testing.assert_allclose(got.values, expected.values, rtol=1e-6, atol=1e-6, equal_nan=True)
