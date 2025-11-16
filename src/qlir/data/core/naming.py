# data/core/naming.py
from __future__ import annotations

"""
Canonical naming rules for on-disk candle datasets.

This module is the single source of truth for:

- How candle filenames are constructed
- How TimeFreq maps to filesystem/metadata resolution strings
- Canonical metadata keys for parquet (or other formats)
- Basic validation of canonical candle filenames

It does NOT know:
- Where files live on disk (dirs, roots)        → see core.paths
- How to infer names from arbitrary files       → see core.infer (or similar)
- How to talk to datasources                    → see data.sources
- How to read/write files                       → handled by I/O layers
"""

from typing import Optional, TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    # Adjust import path if your TimeFreq lives somewhere else
    from qlir.time.timefreq import TimeFreq


# Default extension for canonical candle datasets on disk.
DEFAULT_CANDLES_EXT: str = ".parquet"


# ---------------------------------------------------------------------------
# Resolution string mapping
# ---------------------------------------------------------------------------

def resolution_str(freq: "TimeFreq") -> str:
    """
    Convert a TimeFreq into the canonical resolution string used in filenames
    and metadata (e.g. "1m", "5m", "1h", "1D").

    This is the ONLY place that should define how TimeFreq renders to a
    filesystem/metadata-friendly resolution code.

    Examples of desired outputs (conceptual):
        TimeFreq._1min  -> "1m"
        TimeFreq._5min  -> "5m"
        TimeFreq._1h    -> "1h"
        TimeFreq._1D    -> "1D"

    Implementation notes
    --------------------
    - Wire this up to your actual TimeFreq abstraction.
    - If TimeFreq already has something like `.as_pandas_str()` or
      `.code`, this function should delegate to that and, if necessary,
      normalize into the shorter "1m"/"5m"/"1h"/"1D" forms.
    """
    # Intentionally left for the concrete implementation in your codebase.
    # Keeping this as a stub avoids guessing about your TimeFreq API.
    raise NotImplementedError("Implement resolution_str(...) for your TimeFreq type.")


# ---------------------------------------------------------------------------
# Canonical filename construction
# ---------------------------------------------------------------------------

def candle_filename(
    instrument_id: str,
    resolution: str,
    ext: str = DEFAULT_CANDLES_EXT,
) -> str:
    """
    Build the canonical filename for a candle dataset, without any directory.

    Pattern:
        <instrument_id>_<resolution><ext>

    Where:
        instrument_id : canonical instrument identifier
                        e.g. "sol-perp", "btc-perp"
        resolution    : canonical resolution string
                        e.g. "1m", "5m", "1h", "1D"
        ext           : file extension (default: ".parquet")

    Examples:
        candle_filename("sol-perp", "1m")   -> "sol-perp_1m.parquet"
        candle_filename("btc-perp", "5m")   -> "btc-perp_5m.parquet"
    """
    if not ext.startswith("."):
        raise ValueError(f"Extension must start with '.', got {ext!r}")

    # Zero tolerance for whitespace / empty IDs at this layer.
    instrument_id = instrument_id.strip()
    resolution = resolution.strip()

    if not instrument_id:
        raise ValueError("instrument_id must be a non-empty string.")
    if not resolution:
        raise ValueError("resolution must be a non-empty string.")

    return f"{instrument_id}_{resolution}{ext}"


# ---------------------------------------------------------------------------
# Canonical filename parsing / validation
# ---------------------------------------------------------------------------

def split_candle_stem(stem: str) -> Optional[Tuple[str, str]]:
    """
    Split a canonical candle filename *stem* (no extension) into
    (instrument_id, resolution).

    Expects the canonical pattern:

        <instrument_id>_<resolution>

    Returns:
        (instrument_id, resolution) if the stem matches the expected pattern,
        otherwise None.

    Examples:
        split_candle_stem("sol-perp_1m")   -> ("sol-perp", "1m")
        split_candle_stem("btc-perp_5m")   -> ("btc-perp", "5m")
        split_candle_stem("weird")         -> None
        split_candle_stem("too_many_parts_x_y") -> None
    """
    parts = stem.split("_")
    if len(parts) != 2:
        return None

    instrument_id, resolution = (p.strip() for p in parts)

    if not instrument_id or not resolution:
        return None

    return instrument_id, resolution


def is_canonical_candle_name(filename: str) -> bool:
    """
    Return True if the given filename (with or without extension) appears to
    follow the canonical candle naming convention:

        <instrument_id>_<resolution>[.<ext>]

    This is a lightweight syntactic check; it does not validate that
    the instrument or resolution are *known*, only that they match the pattern.
    """
    # Strip any extension: "sol-perp_1m.parquet" -> "sol-perp_1m"
    stem = filename.rsplit(".", 1)[0]
    return split_candle_stem(stem) is not None


# ---------------------------------------------------------------------------
# Parquet metadata helpers (optional but useful)
# ---------------------------------------------------------------------------

def parquet_metadata(
    instrument_id: str,
    resolution: str,
    *,
    datasource: Optional[str] = None,
    upstream_symbol: Optional[str] = None,
    qlir_version: Optional[str] = None,
) -> dict[bytes, bytes]:
    """
    Build a standard metadata dict for parquet schemas.

    All values are encoded as UTF-8 bytes to match pyarrow expectations.

    Canonical keys:
        - "instrument_id"   : canonical instrument identifier, e.g. "sol-perp"
        - "resolution"      : canonical resolution string, e.g. "1m"
        - "datasource"      : optional datasource name, e.g. "drift"
        - "upstream_symbol" : optional venue-specific symbol, e.g. "SOL-PERP"
        - "qlir_version"    : optional version tag for the QLIR data layout

    This helper is optional but gives you one place to standardize
    on key names and meanings.
    """
    base = {
        "instrument_id": instrument_id,
        "resolution": resolution,
    }

    if datasource is not None:
        base["datasource"] = datasource
    if upstream_symbol is not None:
        base["upstream_symbol"] = upstream_symbol
    if qlir_version is not None:
        base["qlir_version"] = qlir_version

    return {k.encode("utf-8"): v.encode("utf-8") for k, v in base.items()}


def sidecar_metadata(
    instrument_id: str,
    resolution: str,
    *,
    datasource: str | None = None,
    upstream_symbol: str | None = None,
    qlir_version: str | None = None,
    extra: dict | None = None,
) -> dict[str, str]:
    """
    Canonical JSON-sidecar metadata for datasets.

    Mirrors `parquet_metadata` but with string keys (not bytes)
    and JSON-serialization-friendly values.
    """
    base = {
        "instrument_id": instrument_id,
        "resolution": resolution,
    }

    if datasource:       base["datasource"] = datasource
    if upstream_symbol:  base["upstream_symbol"] = upstream_symbol
    if qlir_version:     base["qlir_version"] = qlir_version
    if extra:            base.update(extra)

    return base
