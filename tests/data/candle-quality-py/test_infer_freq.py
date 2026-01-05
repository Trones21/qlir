import logging
import pandas as _pd
import pytest
pytestmark = pytest.mark.local

from qlir.time.timefreq import TimeUnit
log = logging.getLogger(__name__)

from qlir.data.quality.candles.candles import infer_freq, TimeFreq

def test_infer_freq_1min():
    df = _pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:02:00",
                "2025-01-01 00:04:00",
            ]
        }
    )
    expected = TimeFreq(2, TimeUnit.MINUTE)
    assert infer_freq(df) == expected

def test_infer_freq_returns_none_for_insufficient_rows():
    df = _pd.DataFrame({"tz_start": ["2025-01-01 00:00:00"]})
    assert infer_freq(df) is None

def test_infer_freq_returns_none_when_missing_col():
    df = _pd.DataFrame({"not_tz_start": [1, 2, 3]})
    assert infer_freq(df) is None
