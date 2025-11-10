import logging
import pandas as pd
log = logging.getLogger(__name__)

from qlir.data.candle_quality import infer_freq, TimeFreq

def test_infer_freq_1min():
    df = pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:02:00",
                "2025-01-01 00:04:00",
            ]
        }
    )
    expected = TimeFreq(2, "minute")
    assert infer_freq(df) == expected

def test_infer_freq_returns_none_for_insufficient_rows():
    df = pd.DataFrame({"tz_start": ["2025-01-01 00:00:00"]})
    assert infer_freq(df) is None

def test_infer_freq_returns_none_when_missing_col():
    df = pd.DataFrame({"not_tz_start": [1, 2, 3]})
    assert infer_freq(df) is None
