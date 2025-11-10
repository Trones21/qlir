import pandas as pd
import pytest

from qlir.data.candle_quality import _sort_dedupe

def test_sort_dedupe_sorts_and_drops_identical_dupe_on_default_col():
    df = pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:01:00",
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",  # exact dup in OHLCV
            ],
            "open": [2, 1, 1],
            "high": [2, 1, 1],
            "low": [2, 1, 1],
            "close": [2, 1, 1],
            "volume": [20, 10, 10],
        }
    )

    out, dropped = _sort_dedupe(df)

    assert list(out["tz_start"]) == [
        pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
        pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
    ]
    assert dropped == 1
    # we kept the canonical OHLCV for the earlier timestamp
    first = out.iloc[0]
    assert first["open"] == 1
    assert first["volume"] == 10

def test_sort_dedupe_raises_on_conflicting_duplicates():
    df = pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
            ],
            "open": [1, 999],  # mismatch
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [5, 5],
        }
    )

    with pytest.raises(ValueError):
        _sort_dedupe(df)

def test_sort_dedupe_allows_missing_volume_if_other_ohlc_match():
    df = pd.DataFrame(
        {
            "tz_start": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
            ],
            "open": [1, 1],
            "high": [2, 2],
            "low": [0.5, 0.5],
            "close": [1.5, 1.5],
        }
    )

    out, dropped = _sort_dedupe(df)
    assert len(out) == 1
    assert dropped == 1

def test_sort_dedupe_supports_custom_datetime_column():
    df = pd.DataFrame(
        {
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
            ],
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [10, 10],
        }
    )

    out, dropped = _sort_dedupe(df, time_col="timestamp")

    assert len(out) == 1
    assert dropped == 1
    assert "timestamp" in out.columns

def test_sort_dedupe_raises_on_bad_datetime_in_custom_col():
    df = pd.DataFrame(
        {
            "timestamp": ["not-a-date", "2025-01-01 00:00:00"],
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
        }
    )

    with pytest.raises(ValueError):
        _sort_dedupe(df, time_col="timestamp")

def test_sort_dedupe_raises_if_time_col_missing():
    df = pd.DataFrame({"open": [1]})
    with pytest.raises(ValueError):
        _sort_dedupe(df, time_col="tz_start")
