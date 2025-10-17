import pandas as pd
from src.qlir.data.csv import load_ohlcv_from_csv

def test_load_ohlcv_from_csv(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("timestamp,Open,High,Low,Close,Volume\n2024-01-01T00:00:00Z,1,2,0.5,1.5,10\n")
    df = load_ohlcv_from_csv(str(p))
    assert list(df.columns) == ["timestamp","open","high","low","close","volume"]
    assert pd.api.types.is_datetime64tz_dtype(df["timestamp"])
