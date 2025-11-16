from __future__ import annotations
from pathlib import Path
from typing import Optional
import pandas as pd

from qlir import io
from qlir.data.core.instruments import CanonicalInstrument
from ..trash.currently_unused.schema import validate_ohlcv, OhlcvContract
import pyarrow.parquet as pq
import json
import logging
from qlir.data.sources.base import DataSource
from qlir.time.timefreq import TimeFreq

from qlir.data.sources.drift.fetch import get_all_candles

log = logging.getLogger(__name__)

def candles_from_disk_or_network(
    *,
    file_uri: Path | None,
    symbol: CanonicalInstrument | None,
    base_resolution: TimeFreq | None,
    source: Optional[DataSource] = None,
):
    """
    Load candles:

    1. If file_uri exists → load from disk (fast path)
    2. Else:
        - If source is provided → fetch from network
        - Else → raise error instructing user to specify a datasource
    """

    # ----- 1. Disk fast path --------------------------------------------------
    if file_uri is not None and file_uri.exists():
        log.info(f"Loading candles from disk: {file_uri}")
        return io.read(file_uri)

    # ----- 2. Disk missed, user must specify a network source ----------------
    if source is None:
        raise ValueError(
            f"File not found at {file_uri}. "
            f"You must specify a datasource (e.g. source=DataSource.DRIFT)."
        )

    # ----- 3. Fetch from network ---------------------------------------------
    if source is DataSource.DRIFT:
        log.info(f"Fetching candles from Drift base_resolution={base_resolution}")
        
        return get_all_candles(symbol)

    if source is DataSource.KAIKO:
        log.info("Fetching candles from Kaiko")
        raise NotImplementedError("TODO: implement this")

    if source is DataSource.HELIUS:
        log.info("Fetching candles from Helius")
        raise NotImplementedError("TODO: implement this")

    if source is DataSource.MOCK:
        log.info("Returning mock candles")
        raise NotImplementedError("TODO: implement this")

    raise ValueError(f"Unknown datasource: {source}")



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
    from qlir.io.reader import read
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

def get_symbol(file_path: Path) -> str | None:
    """
    Infer trading symbol for a dataset file.
    Standard naming:  <dir>/<symbol>_<resolution>.<ext>
    Example: data/SOL-PERP_1m.parquet
    """
    p = file_path

    # 1. Try Parquet metadata
    if p.suffix == ".parquet":
        try:
            table = pq.read_table(p)
            meta = table.schema.metadata or {}
            if b"symbol" in meta:
                return meta[b"symbol"].decode()
        except Exception:
            pass

    # 2. Try sidecar metadata file
    meta_path = p.with_suffix(".meta.json")
    if meta_path.exists():
        try:
            with open(meta_path, "r") as f:
                meta = json.load(f)
            if "symbol" in meta:
                return meta["symbol"]
        except Exception:
            pass

    # 3. Derive from naming convention: symbol_resolution.ext
    stem = p.stem  # e.g. SOL-PERP_1m
    if "_" in stem:
        symbol, _ = stem.split("_", 1)
        return symbol

    # 4. Fallback: check column (rare)
    if p.suffix in (".csv", ".json"):
        try:
            df = pd.read_json(p) if p.suffix == ".json" else pd.read_csv(p)
            if "symbol" in df.columns:
                uniq = df["symbol"].dropna().unique()
                if len(uniq) == 1:
                    return uniq[0]
        except Exception:
            pass

    return None


def get_resolution(file_path: str) -> str | None:
    """
    Infer resolution (e.g. 1m, 5m, 1h) from <symbol>_<resolution>.<ext>
    """
    p = Path(file_path)
    stem = p.stem
    if "_" in stem:
        _, resolution = stem.split("_", 1)
        return resolution
    return None

