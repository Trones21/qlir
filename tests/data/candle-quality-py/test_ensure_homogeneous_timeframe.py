import pandas as pd
import pytest
pytestmark = pytest.mark.local

from qlir.data.quality.candles.candles import ensure_homogeneous_candle_size


def test_homogeneous_passes_for_equal_intervals():
    df = pd.DataFrame(
        {
            "tz_start": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 00:01:00",
                    "2025-01-01 00:02:00",
                ],
                utc=True,
            )
        }
    )
    ensure_homogeneous_candle_size(df)  # should not raise


def test_homogeneous_raises_for_mixed_intervals():
    df = pd.DataFrame(
        {
            "tz_start": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 00:01:00",
                    "2025-01-01 00:03:00",  # 2-minute gap breaks it
                ],
                utc=True,
            )
        }
    )
    with pytest.raises(ValueError):
        ensure_homogeneous_candle_size(df)


def test_homogeneous_allows_short_series():
    df = pd.DataFrame({"tz_start": ["2025-01-01 00:00:00"]})
    ensure_homogeneous_candle_size(df)
