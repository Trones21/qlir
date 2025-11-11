import pandas as pd
import pytest

from qlir.data.candle_quality import validate_candles, CandlesDQReport
from qlir.time.timefreq import TimeFreq, TimeUnit
import logging

from qlir.utils.logdf import logdf

log = logging.getLogger(__name__)


def _make_df_with_gap():
    return pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:02:00",  # gap at 00:01
                "2025-01-01 00:03:00",
            ],
            "open": [1, 2, 3],
            "high": [1, 2, 3],
            "low": [1, 2, 3],
            "close": [1, 2, 3],
        }
    )

def test_validate_candles_reports_gap():
    df = _make_df_with_gap()
    fixed, report = validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))

    # there was exactly 1 gap
    assert report.n_gaps == 1
    assert report.missing_starts == [pd.Timestamp("2025-01-01 00:01:00", tz="UTC")]


def _make_df_with_exact_duplicate():
    return pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:01:00", 
                "2025-01-01 00:02:00",
                "2025-01-01 00:02:00",
            ],
            "open": [1, 2, 10, 10],
            "high": [1, 2, 10, 10],
            "low": [1, 2, 10, 10],
            "close": [1, 2, 10, 10],
        }
    )

def test_validate_candles_drop_exact_ohclv_duplicate():
    df = _make_df_with_exact_duplicate()
    fixed, report = validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))
    assert report.n_dupes_dropped == 1  

def test_validate_candles_raise_on_dup_dt_but_differing_candle():
    df = _make_df_with_exact_duplicate()
    
    #Edit one of the duplicate rows so that tz_start is the same, but one of ohclv values differs (bad data) 
    df.loc[3, "open"] = 99

    with pytest.raises(ValueError):
        fixed, report = validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))
    