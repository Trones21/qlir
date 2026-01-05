
import pytest
pytestmark = pytest.mark.local

import pandas as _pd

from qlir.data.resampling.generate_candles import (
    generate_candles_from_1m,
    generate_candles,
)
from qlir.time.timefreq import TimeFreq, TimeUnit
from qlir.logging.logdf import logdf

import logging 
log = logging.getLogger(__name__)


def make_1m_df(n=10, start="2025-01-01 00:00:00Z"):
    """Build a tiny homogeneous 1m dataframe with tz_start like your infer_freq expects."""
    idx = _pd.date_range(start=start, periods=n, freq="1min")  # 1-minute
    df = _pd.DataFrame(
        {
            "tz_start": idx,   # important for infer_freq
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
        }
    )
    return df


def test_generate_candles_from_1m_happy_path():
    df = make_1m_df(n=60)  # 1 hour of 1m data

    # Set some values that will end up on the new first row
    expected_high_5m = 99
    expected_low_5m = 0.1
    expected_open_5m = 55
    expected_close_5m = 33
    df.loc[2, 'high'] = expected_high_5m
    df.loc[1, 'low'] = expected_low_5m
    df.loc[0, 'open'] = expected_open_5m
    df.loc[4, 'close'] = expected_close_5m


    # ---------- Act -----------
    # we want 5m and 15m
    out = generate_candles_from_1m(
        df,
        out_unit=TimeUnit.MINUTE,
        out_agg_candle_sizes=[5, 15],
        dt_col="tz_start",
    )

    # ---------- Assert -----------
    ## Ensure both aggregations are there
    assert "5min" in out
    assert "15min" in out

    five = out["5min"]
    fifteen = out["15min"]
    
    # 60 minutes → 12 bars of 5m
    assert len(five) == 12
    # 60 minutes → 4 bars of 15m
    assert len(fifteen) == 4

    # Ensure the first row of 5min agg'd properly
    row = five.iloc[0]
    assert row['open'] == expected_open_5m
    assert row['close'] == expected_close_5m
    assert row['high'] == expected_high_5m
    assert row['low'] == expected_low_5m    

    # metadata present
    assert "meta__candle_freq" in five.columns
    assert "meta__derived_from_freq" in five.columns


def test_generate_candles_from_1m_raises_on_gaps():
    df = make_1m_df(n=10)
    # drop one row to create a gap
    df = df.drop(index=[3])

    with pytest.raises(ValueError) as exc:
        generate_candles_from_1m(
            df,
            out_unit=TimeUnit.MINUTE,
            out_agg_candle_sizes=[5],
            dt_col="tz_start",
        )

    # optional: check the message payload
    assert "gaps" in str(exc.value)
    log.info("✅ generate_candles raised ValueError, logged critical, and printed dataframe as expected")


def test_generate_candles_general_happy_path():
    # make hourly data
    idx = _pd.date_range("2025-01-01 00:00:00Z", periods=24, freq="1h")
    df = _pd.DataFrame(
        {
            "tz_start": idx,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10,
        }
    )

    # in_unit is 1 hour, we want 2H and 4H
    out = generate_candles(
        df,
        in_unit=TimeFreq(1, TimeUnit.HOUR),
        out_unit=TimeUnit.HOUR,
        counts=[2, 4],
        dt_col="tz_start",
    )

    assert "2h" in out
    assert "4h" in out

    twoh = out["2h"]
    fourh = out["4h"]

    # 24h → 12 bars of 2h
    assert len(twoh) == 12
    # 24h → 6 bars of 4h
    assert len(fourh) == 6


def test_generate_candles_in_unit_mismatch_raises():
    # we actually make 1h data
    idx = _pd.date_range("2025-01-01 00:00:00Z", periods=10, freq="1h")
    df = _pd.DataFrame(
        {
            "tz_start": idx,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10,
        }
    )

    # but we lie and say it's 1 minute
    with pytest.raises(ValueError):
        generate_candles(
            df,
            in_unit=TimeFreq(1, TimeUnit.MINUTE),
            out_unit=TimeUnit.HOUR,
            counts=[2],
            dt_col="tz_start",
        )
    log.info("✅ generate_candles raised ValueError, logged critical, and printed dataframe as expected")
