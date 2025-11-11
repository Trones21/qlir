# tests/test_ohlcv_loader.py
import pandas as pd
from qlir.data.load import load_ohlcv
from qlir.io import write
import pytest
pytestmark = pytest.mark.local

def _seed_df():
    return pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z"],
            "Open": [1], "High": [2], "Low": [0.5], "Close": [1.5], "Volume": [10],
        }
    )

def test_load_ohlcv_csv(tmp_path):
    df = _seed_df()
    p = tmp_path / "x.csv"
    write(df, p)  # our writer (calls DataFrame.to_csv)
    got = load_ohlcv(p)
    assert list(got.columns)[:6] == ["timestamp","open","high","low","close","volume"]
    assert pd.api.types.is_datetime64tz_dtype(got["timestamp"]) # type: ignore[attr-defined]

def test_load_ohlcv_parquet(tmp_path):
    pyarrow = __import__("pytest").importorskip("pyarrow")
    df = _seed_df()
    p = tmp_path / "x.parquet"
    write(df, p, compression="snappy")
    got = load_ohlcv(p)
    assert list(got.columns)[:6] == ["timestamp","open","high","low","close","volume"]
    assert pd.api.types.is_datetime64tz_dtype(got["timestamp"]) # type: ignore[attr-defined]

def test_load_ohlcv_json(tmp_path):
    df = _seed_df()
    p = tmp_path / "x.json"
    write(df, p)  # defaults to records JSON
    got = load_ohlcv(p)
    assert list(got.columns)[:6] == ["timestamp","open","high","low","close","volume"]
    assert pd.api.types.is_datetime64tz_dtype(got["timestamp"]) # type: ignore[attr-defined]
