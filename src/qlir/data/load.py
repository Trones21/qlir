from __future__ import annotations
from pathlib import Path
from typing import Optional
import pandas as pd
from .schema import validate_ohlcv, OhlcvContract
from qlir.io.reader import read

def load_ohlcv(
    path: str | Path,
    *,
    require_volume: bool = False,
    allow_partials: bool = True,
    index_by: Optional[str] = None,         # 'tz_end' or 'tz_start'
    drop_partials: bool = False,            # convenience: filter tz_end.isna()
) -> pd.DataFrame:
    """
    Load a canonical OHLCV file and validate it against the contract.
    This does NOT normalize — it only validates and (optionally) filters/indexes.
    """
    df = read(path)

    # Light coercions so files that are "almost right" pass (no normalization here)
    for col in ("tz_start","tz_end"):
        if col in df:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    for c in ("open","high","low","close","volume"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    validate_ohlcv(df, cfg=OhlcvContract(require_volume=require_volume, allow_partials=allow_partials))

    if drop_partials and "tz_end" in df:
        df = df[df["tz_end"].notna()]

    if index_by:
        if index_by not in ("tz_end", "tz_start"):
            raise ValueError("index_by must be 'tz_end' or 'tz_start'")
        if index_by not in df.columns:
            raise KeyError(f"index_by='{index_by}' not found.")
        df = df.set_index(index_by)

    return df
