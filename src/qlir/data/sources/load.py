from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as _pd

from qlir import io
from qlir.data.core.datasource import DataSource
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.sources.drift.fetch.fills import get_all_candles as get_all_fill_candles
from qlir.data.sources.drift.fetch.oracle import get_all_candles as get_all_oracle_candles
from qlir.data.sources.drift.fetch.oracleorfill import OracleOrFill
from qlir.time.timefreq import TimeFreq

from ..trash.currently_unused.schema import OhlcvContract, validate_ohlcv

log = logging.getLogger(__name__)

from enum import Enum, auto

from qlir.data.quality.candles.candles import (
    log_candle_dq_issues,
    run_candle_quality,
)


class DiskOrNetwork(Enum):
    DISK = auto()
    NETWORK = auto()

def candles_from_disk_or_network(
    *,
    disk_or_network: DiskOrNetwork, 
    file_uri: Path | None,
    symbol: CanonicalInstrument | None,
    base_resolution: TimeFreq | None,
    datasource: Optional[DataSource] = None,
    oracle_or_fill: OracleOrFill
):
    """
    Load candles either from a local file or from an external datasource.

    Parameters
    ----------
    disk_or_network : DiskOrNetwork
        Selects the source of candle data (DISK or NETWORK).
    file_uri : Path, optional
        Path to the local candle file when loading from disk.
    symbol : CanonicalInstrument, optional
        Instrument symbol required when fetching from the network.
    base_resolution : TimeFreq, optional
        Base candle resolution required for network fetch.
    datasource : DataSource, optional
        Venue datasource to fetch from.

    Returns
    -------
    Any
        The candle data loaded from disk or network.

    Raises
    ------
    ValueError
        If required parameters for the selected mode are missing.
    """

    if disk_or_network is DiskOrNetwork.DISK:    
        return candles_from_disk_via_explicit_filepath(file_uri, freq=base_resolution, oracle_or_fill=oracle_or_fill)

    if disk_or_network is DiskOrNetwork.NETWORK:
        
        if any(x is None for x in [symbol, base_resolution, datasource]):
            log.info("Symbol (CanonicalInstrument), base_resolution (TimeFreq), and datasource (Datasource) must be passed when fetching from network")

        return candles_from_network(symbol=symbol, base_resolution=base_resolution, source=datasource, oracle_or_fill=oracle_or_fill)


def candles_from_disk_via_built_filepath(
    symbol: CanonicalInstrument,
    base_resolution: TimeFreq,
    datasource: Optional[DataSource],
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
):
    raise NotImplementedError()
    # Your internal path-builder (whatever you already use)
    file_uri = build_candle_path(symbol, base_resolution, datasource)

    return candles_from_disk_validated(
        file_uri=file_uri,
        freq=base_resolution,
        max_abs_range=max_abs_range,
        max_rel_range=max_rel_range,
    )


def candles_from_disk_via_explicit_filepath(
    file_uri: str | Path,
    freq: TimeFreq,
    oracle_or_fill: OracleOrFill,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
):
    return candles_from_disk_validated(
        file_uri=file_uri,
        freq=freq,
        oracle_or_fill=oracle_or_fill,
        max_abs_range=max_abs_range,
        max_rel_range=max_rel_range,
    )

def candles_from_disk_validated(
    file_uri: str | Path,
    freq: TimeFreq,
    *,
    oracle_or_fill: OracleOrFill,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
):
    """
    Load + validate + log candle DQ in one place.
    """

    # --- normalize input
    file_uri = io.writer._prep_path(file_uri)
    if not file_uri.exists():
        raise ValueError(f"File not found at {file_uri}")

    # --- load raw
    log.info("Loading candles from disk: %s", file_uri)
    raw_df = io.read(file_uri)

    # --- run DQ
    clean_df, report, issues = run_candle_quality(
        raw_df,
        freq=freq,
        max_abs_range=max_abs_range,
        max_rel_range=max_rel_range,
    )

    # --- log everything
    log_candle_dq_issues(
        report,
        context=f"file:{file_uri}",
    )

    return clean_df, report, issues


def candles_from_network(source, symbol, base_resolution, oracle_or_fill: OracleOrFill):

    # ----- 3. Fetch from network ---------------------------------------------
    if source is DataSource.DRIFT:
        # Oracle or fill param has been decided, im just calling different funcs depending on the value of the param passed
        if oracle_or_fill is OracleOrFill.oracle:
            log.info(f"Fetching oracle candles from Drift base_resolution={base_resolution}")
            return get_all_oracle_candles(symbol=symbol, base_resolution=base_resolution, oracle_or_fill=oracle_or_fill)
        
        if oracle_or_fill is OracleOrFill.fill:
            log.info(f"Fetching fill candles from Drift base_resolution={base_resolution}")
            return get_all_fill_candles(symbol=symbol, base_resolution=base_resolution, oracle_or_fill=oracle_or_fill)
        
        raise ValueError("Oracle or Fill was not passed to candles_from_network, therefore we do not know what type of data to get")

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


def old_load_ohlcv(
    path: str | Path,
    *,
    require_volume: bool = False,
    allow_partials: bool = True,
    index_by: Optional[str] = None,         # 'tz_end' or 'tz_start'
    drop_partials: bool = False,            # convenience: filter tz_end.isna()
) -> _pd.DataFrame:
    """
    Load a canonical OHLCV file and validate it against the contract.
    This does NOT normalize — it only validates and (optionally) filters/indexes.
    """
    from qlir.io.reader import read
    df = read(path)

    # Light coercions so files that are "almost right" pass (no normalization here)
    for col in ("tz_start","tz_end"):
        if col in df:
            df[col] = _pd.to_datetime(df[col], utc=True, errors="coerce")
    for c in ("open","high","low","close","volume"):
        if c in df:
            df[c] = _pd.to_numeric(df[c], errors="coerce")

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

# def get_symbol(file_path: Path) -> str | None:
#     """
#     Infer trading symbol for a dataset file.
#     Standard naming:  <dir>/<symbol>_<resolution>.<ext>
#     Example: data/SOL-PERP_1m.parquet
#     """
#     p = file_path

#     # 1. Try Parquet metadata
#     if p.suffix == ".parquet":
#         try:
#             table = pq.read_table(p)
#             meta = table.schema.metadata or {}
#             if b"symbol" in meta:
#                 return meta[b"symbol"].decode()
#         except Exception:
#             pass

#     # 2. Try sidecar metadata file
#     meta_path = p.with_suffix(".meta.json")
#     if meta_path.exists():
#         try:
#             with open(meta_path, "r") as f:
#                 meta = json.load(f)
#             if "symbol" in meta:
#                 return meta["symbol"]
#         except Exception:
#             pass

#     # 3. Derive from naming convention: symbol_resolution.ext
#     stem = p.stem  # e.g. SOL-PERP_1m
#     if "_" in stem:
#         symbol, _ = stem.split("_", 1)
#         return symbol

#     # 4. Fallback: check column (rare)
#     if p.suffix in (".csv", ".json"):
#         try:
#             df = _pd.read_json(p) if p.suffix == ".json" else _pd.read_csv(p)
#             if "symbol" in df.columns:
#                 uniq = df["symbol"].dropna().unique()
#                 if len(uniq) == 1:
#                     return uniq[0]
#         except Exception:
#             pass

#     return None


# def get_resolution(file_path: str) -> str | None:
#     """
#     Infer resolution (e.g. 1m, 5m, 1h) from <symbol>_<resolution>.<ext>
#     """
#     p = Path(file_path)
#     stem = p.stem
#     if "_" in stem:
#         _, resolution = stem.split("_", 1)
#         return resolution
#     return None

