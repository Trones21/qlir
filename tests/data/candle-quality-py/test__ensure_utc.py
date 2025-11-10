import pandas as pd
import pytest

from qlir.data.candle_quality import _ensure_utc


def test_ensure_utc_parses_and_sets_utc():
    s = pd.Series(["2025-01-01 00:00:00", "2025-01-01 00:01:00"])
    out = _ensure_utc(s)
    assert out.dt.tz is not None
    assert str(out.dt.tz) == "UTC"
    assert out.iloc[0] == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")


def test_ensure_utc_raises_on_invalid_timestamp():
    s = pd.Series(["2025-01-01 00:00:00", "not-a-ts"])
    with pytest.raises(ValueError):
        _ensure_utc(s)
