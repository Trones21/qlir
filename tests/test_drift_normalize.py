import pandas as pd
from src.qlir.data.drift import _rename_to_canonical

def test_rename_variants():
    raw = pd.DataFrame([{"ts": 1_700_000_000, "fillOpen":1, "fillHigh":2, "fillLow":0.5, "fillClose":1.5, "baseVolume":10}])
    df = _rename_to_canonical(raw)
    assert all(c in df.columns for c in ["timestamp","open","high","low","close","volume"])
