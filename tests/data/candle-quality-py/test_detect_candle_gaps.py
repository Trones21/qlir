import pandas as pd
import pytest
pytestmark = pytest.mark.local

from qlir.data.quality.candles import detect_candle_gaps, TimeFreq
import logging
from qlir.time.timefreq import TimeUnit

log = logging.getLogger(__name__)


def _candles(timestamps):
    return pd.DataFrame(
        {
            "tz_start": timestamps,
            "open": [1.0] * len(timestamps),
            "high": [1.0] * len(timestamps),
            "low": [1.0] * len(timestamps),
            "close": [1.0] * len(timestamps),
            "volume": [1.0] * len(timestamps),
        }
    )


def test_detect_candle_gaps_none_missing_minutely():
    df = _candles(
        [
            "2025-01-01 00:00:00",
            "2025-01-01 00:01:00",
            "2025-01-01 00:02:00",
        ]
    )
    tf = TimeFreq(count=1, unit=TimeUnit.MINUTE)
    missing = detect_candle_gaps(df, freq=tf)
    assert missing == []


def test_detect_candle_gaps_finds_gap_minutely():
    df = _candles(
        [
            "2025-01-01 00:00:00",
            "2025-01-01 00:02:00",
            "2025-01-01 00:03:00",
        ]
    )
    tf = TimeFreq(count=1, unit=TimeUnit.MINUTE)
    missing = detect_candle_gaps(df, freq=tf)
    assert missing == [pd.Timestamp("2025-01-01 00:01:00", tz="UTC")]


def test_detect_candle_gaps_returns_empty_when_no_freq_or_too_short():
    df = _candles(["2025-01-01 00:00:00"])
    tf = TimeFreq(count=1, unit=TimeUnit.MINUTE)
    # too short -> []
    assert detect_candle_gaps(df, freq=tf) == []
    # no freq -> []
    assert detect_candle_gaps(df, freq=None) == []
