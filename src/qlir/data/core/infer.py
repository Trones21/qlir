# data/core/infer.py
from __future__ import annotations
import logging
log = logging.getLogger(__name__)

"""
Helpers for *inferring* dataset identity from existing files.

This module is intentionally "diagnostic" and not on the hot path:
- It is used for debugging, importing external data, or sanity checks.
- Normal workflows should know instrument_id, resolution, and datasource
  from configuration / code, not by inferring from files.

Inference strategy (for instrument_id / resolution / datasource / upstream_symbol):

1. Parquet embedded metadata (if available)
2. Sidecar JSON metadata: <file>.meta.json
3. Canonical filename pattern: <instrument_id>_<resolution>.<ext>
4. Optional column scan (for CSV/JSON) as a last resort
"""

from pathlib import Path
from typing import Any, Optional, Dict, Tuple

import json

import pandas as pd
import pyarrow.parquet as pq

from qlir.data.core.naming import split_candle_stem


# ---------------------------------------------------------------------------
# Low-level helpers: read metadata sources
# ---------------------------------------------------------------------------

def _read_parquet_metadata(path: Path) -> Dict[str, str]:
    """
    Read parquet schema metadata and return it as a {str: str} dict.

    If the file is not parquet or metadata is missing/unreadable, returns {}.
    """
    if path.suffix != ".parquet":
        return {}

    try:
        table = pq.read_table(path)
        meta = table.schema.metadata or {}
        # Decode bytes -> str
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in meta.items()}
    except Exception:
        return {}


def _read_sidecar_meta(path: Path) -> Dict[str, Any]:
    """
    Read sidecar .meta.json next to the dataset, if it exists.

    Returns {} on any error or if file is missing.
    """
    meta_path = path.with_suffix(".meta.json")
    if not meta_path.exists():
        return {}

    try:
        return json.loads(meta_path.read_text())
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Instrument ID inference (canonical)
# ---------------------------------------------------------------------------

def infer_instrument_id(file_path: str | Path) -> Optional[str]:
    """
    Infer the canonical instrument_id for a dataset file.

    Resolution order:
        1. Parquet metadata: "instrument_id", then legacy "symbol"
        2. Sidecar JSON: "instrument_id", then legacy "symbol"
        3. Canonical filename stem: <instrument_id>_<resolution>.<ext>
        4. Column scan for CSV/JSON: "instrument_id" or "symbol" column
           with a single unique non-null value

    Returns:
        instrument_id (e.g. "sol-perp") or None if it cannot be inferred.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # 1. Parquet metadata
    meta = _read_parquet_metadata(file_path)
    if "instrument_id" in meta:
        return meta["instrument_id"]
    if "symbol" in meta:
        # Legacy / upstream symbol; we treat it as instrument_id when nothing else is known
        return meta["symbol"]

    # 2. Sidecar JSON metadata
    sidecar = _read_sidecar_meta(file_path)
    if "instrument_id" in sidecar:
        return str(sidecar["instrument_id"])
    if "symbol" in sidecar:
        return str(sidecar["symbol"])

    # 3. Canonical filename stem: <instrument_id>_<resolution>.<ext>
    stem = file_path.stem  # e.g. "sol-perp_1m"
    split = split_candle_stem(stem)
    if split is not None:
        instrument_id, _ = split
        return instrument_id

    # 4. Column scan for CSV/JSON (rare)
    if file_path.suffix in (".csv", ".json"):
        try:
            if file_path.suffix == ".json":
                df = pd.read_json(file_path)
            else:
                df = pd.read_csv(file_path)

            for col in ("instrument_id", "symbol"):
                if col in df.columns:
                    uniq = df[col].dropna().unique()
                    if len(uniq) == 1:
                        return str(uniq[0])
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Resolution inference
# ---------------------------------------------------------------------------

def infer_resolution(file_path: str | Path) -> Optional[str]:
    """
    Infer the resolution string for a dataset file (e.g. "1m", "5m", "1h").

    Resolution order:
        1. Parquet metadata: "resolution"
        2. Sidecar JSON: "resolution"
        3. Canonical filename stem: <instrument_id>_<resolution>.<ext>

    Returns:
        resolution string or None if it cannot be inferred.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # 1. Parquet metadata
    meta = _read_parquet_metadata(file_path)
    if "resolution" in meta:
        return meta["resolution"]

    # 2. Sidecar JSON metadata
    sidecar = _read_sidecar_meta(file_path)
    if "resolution" in sidecar:
        return str(sidecar["resolution"])

    # 3. Canonical filename stem
    stem = file_path.stem
    split = split_candle_stem(stem)
    if split is not None:
        _, resolution = split
        return resolution

    return None



# def inferred_resolution_to_timefreq(inferred_resolution: str) -> TimeFreq:
#     # Quick type check in case pylance is off / type:ingore
#     if type(inferred_resolution) != str:
#         log.warning(f" Non-string value was passed to inferred_resolution_to_timefreq. Passed type is: {type(inferred_resolution)}")
#     if is_canonical_resolution_str(inferred_resolution):
#         TimeFreq.from_canonical_resolution_str()
#         return 
    
#     raise(ValueError(f"{inferred_resolution} is not a canonical resolution string, conversion failed (non-canonical conversion not implemented)"))

# ---------------------------------------------------------------------------
# Datasource / upstream symbol inference (optional)
# ---------------------------------------------------------------------------

def infer_datasource(file_path: str | Path) -> Optional[str]:
    """
    Infer datasource name for a dataset file.

    Resolution order:
        1. Parquet metadata: "datasource"
        2. Sidecar JSON: "datasource"
        3. Parent directory name (e.g. <root>/<datasource>/<file>)

    Returns:
        datasource string (e.g. "drift") or None.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # 1. Parquet metadata
    meta = _read_parquet_metadata(file_path)
    if "datasource" in meta:
        return meta["datasource"]

    # 2. Sidecar JSON metadata
    sidecar = _read_sidecar_meta(file_path)
    if "datasource" in sidecar:
        return str(sidecar["datasource"])

    # 3. Parent directory name (best-effort)
    parent = file_path.parent
    if parent.name:
        return parent.name

    return None


def infer_upstream_symbol(file_path: str | Path) -> Optional[str]:
    """
    Infer the venue-specific upstream symbol for this dataset, if available.

    Resolution order:
        1. Parquet metadata: "upstream_symbol"
        2. Sidecar JSON: "upstream_symbol"
        3. Legacy fallbacks: "symbol" metadata if `instrument_id` is also present

    Returns:
        upstream symbol (e.g. "SOL-PERP", "SOL/USDC") or None.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    meta = _read_parquet_metadata(file_path)
    if "upstream_symbol" in meta:
        return meta["upstream_symbol"]

    sidecar = _read_sidecar_meta(file_path)
    if "upstream_symbol" in sidecar:
        return str(sidecar["upstream_symbol"])

    # Optional legacy fallback: if instrument_id exists separately,
    # we can treat "symbol" as upstream_symbol instead of canonical.
    if "instrument_id" in meta and "symbol" in meta:
        return meta["symbol"]
    if "instrument_id" in sidecar and "symbol" in sidecar:
        return str(sidecar["symbol"])

    return None


# ---------------------------------------------------------------------------
# High-level convenience
# ---------------------------------------------------------------------------

def infer_dataset_identity(file_path: str | Path) -> Dict[str, Optional[str]]:
    """
    Infer as much identity as possible from a dataset file.

    Returns a dict with keys:
        - "instrument_id"
        - "resolution"
        - "datasource"
        - "upstream_symbol"

    Any field that cannot be inferred will be None.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    instrument_id = infer_instrument_id(file_path)
    resolution = infer_resolution(file_path)
    datasource = infer_datasource(file_path)
    upstream_symbol = infer_upstream_symbol(file_path)

    return {
        "instrument_id": instrument_id,
        "resolution": resolution,
        "datasource": datasource,
        "upstream_symbol": upstream_symbol,
    }
