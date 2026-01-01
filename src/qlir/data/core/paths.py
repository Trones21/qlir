# data/core/paths.py
from __future__ import annotations
import logging
log = logging.getLogger(__name__)

from qlir.time.timefreq import TimeFreq

"""
Canonical disk layout for QLIR datasets.

This module defines *where* datasets live on disk, but performs no
reading or writing of data. All I/O is delegated to the top-level `io/`
module.

Rules:
------
Datasets live under a user-provided root directory (default: ~/qlir_data),
organized by datasource, and named using the canonical filename pattern
defined in `data.core.naming`.

    <root>/<datasource>/<instrument_id>_<resolution>.parquet

Examples:
    ~/qlir_data/drift/sol-perp_1m.parquet
    ~/qlir_data/helius/btc-perp_5m.parquet

This module centralizes all path construction in one place.
"""

from pathlib import Path
from typing import Optional

from qlir.data.core.naming import candle_filename


# ---------------------------------------------------------------------------
# Data root resolution
# ---------------------------------------------------------------------------

def get_data_root(user_root: Optional[Path | str] = None) -> Path:
    """
    Resolve the root directory for all on-disk datasets.

    Priority:
        1. Explicit `user_root` provided by caller.
        2. Environment variable `QLIR_DATA_ROOT` (if set).
        3. Default: ~/qlir_data

    This function does *not* create the directory; callers may choose
    to create it via `ensure_dir` or let the `io/` layer handle creation.
    """
    if user_root is not None:
        return Path(user_root).expanduser().resolve()

    import os
    env_root = os.environ.get("QLIR_DATA_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    # Default fallback
    return Path("~").expanduser().joinpath("qlir_data").resolve()


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def from_canonical_data_root(path_ext: Optional[str | Path] = None,
                              user_root: Optional[str | Path] = None) -> Path:
    """
    Return an absolute path rooted under the canonical data root.

    If `path_ext` is absolute, it is returned unchanged.
    If relative, it becomes: get_data_root(user_root) / path_ext
    """
    root = get_data_root(user_root)

    if path_ext is None:
        log.info(
        "from_canonical_data_root. path_ext param was none, therefore returning root: %s", root)
        return root

    p = Path(path_ext)

    log.info(
        "from_canonical_data_root: root=%s, ext=%s, final=%s",
        root,
        path_ext,
        p
    )
    if p.is_absolute():
        return p

    return root / p


def datasource_dir(
    datasource: str,
    *,
    user_root: Optional[Path | str] = None,
) -> Path:
    """
    Get the directory for a specific datasource under the data root.

    Example:
        datasource_dir("drift") -> ~/qlir_data/drift
    """
    root = get_data_root(user_root)
    return root / datasource


def ensure_dir(path: Path) -> None:
    """
    Ensure a directory exists. This is the only filesystem-affecting
    function in this module and is safe: directory creation only,
    no file I/O, no writes.

    Callers may use this before delegating actual reads/writes to `io/`.
    """
    path.mkdir(parents=True, exist_ok=True)


def get_agg_dir_path(datasource:str, endpoint:str, symbol: str, interval: str, limit: int) -> Path:
    root = get_data_root()
    path = (Path(root)/datasource/endpoint/"agg"/symbol/interval/f"limit={limit}"/"parts")
    if not Path.exists(path):
        raise NotADirectoryError(f"Directory not Found: {path}")
    return path

def get_raw_responses_dir_path(datasource:str, endpoint:str, symbol: str, interval: str, limit: int) -> Path:
    root = get_data_root()
    path = (Path(root)/datasource/endpoint/"raw"/symbol/interval/f"limit={limit}"/"responses")
    if not Path.exists(path):
        raise NotADirectoryError(f"Directory not Found: {path}")
    return path

def get_raw_manifest_path(datasource:str, endpoint:str, symbol: str, interval: str, limit: int) -> Path:
    root = get_data_root()
    path = (Path(root)/datasource/endpoint/"raw"/symbol/interval/f"limit={limit}"/"manifest.json")
    if not Path.exists(path):
        raise FileNotFoundError(f"Manifest not Found at {path}")
    return path

def get_symbol_interval_limit_raw_dir(data_root: Path, datasource: str, endpoint:str, symbol: str, interval: str, limit:int) -> Path:
    """Containseverything the data server interacts with:
        - /responses        # the raw data
        - /claims           # locks 
        - manifest.json     # metadata for each slice (including those without responses yet)
        - manifest.delta    # delta log of manifest.json (batching for perf reasons -- manifest.json can be 100's of MB, and these updates would happen multiple times per second on inital fetch loop)
        - /logs             # logs (currently only for the delta log process, not the main fetching worker)
    """
    root = get_data_root(data_root)
    sym_interval_limit_dir = (
        Path(root)
        .joinpath(datasource, endpoint, "raw", symbol, interval, f"limit={limit}")
        .resolve()
    )
    return sym_interval_limit_dir


# ---------------------------------------------------------------------------
# Canonical dataset path builders
# ---------------------------------------------------------------------------

def candles_path(
    *,
    instrument_id: str,
    resolution: TimeFreq,
    datasource: str,
    dir_suffix_str: str, 
    user_root: Optional[Path | str] = None,
    ext: str = ".parquet",
) -> Path:
    """
    Compute the canonical dataset path for a candle file.

    Layout:
        <root>/<datasource>/<instrument_id>_<resolution>.parquet

    Parameters
    ----------
    instrument_id : str
        Canonical instrument identifier (e.g. "sol-perp", "btc-perp")
    resolution : TimeFreq
    datasource : str
        Name of the datasource (e.g. "drift", "helius", "kaiko")
    dir_suffix_str:
        This is the full dir path -- so if you want the file at /DRIFT/candles/oracle/, you would pass /candles/oracle/ (DRIFT portion is arleady taken care of via datsource param )
    user_root : Path | str | None
        Overrides the data root. If None, `get_data_root` is used.
    ext : str
        File extension (default: ".parquet")

    Returns
    -------
    Path
        Full canonical path to the dataset file.
    """
    if len(dir_suffix_str) > 0:
        if not (dir_suffix_str.startswith("/") and dir_suffix_str.endswith("/")):
            raise ValueError("dir_suffix_str should be wrapped in /. got %s", dir_suffix_str)
    dir_from_root = datasource + dir_suffix_str
    ds_dir = datasource_dir(dir_from_root, user_root=user_root)
    filename = candle_filename(instrument_id, resolution, ext=ext)
    return ds_dir / filename


# ---------------------------------------------------------------------------
# Optional dir creation helpers for callers (non-I/O for files)
# ---------------------------------------------------------------------------

def ensure_candles_dir(
    datasource: str,
    *,
    user_root: Optional[Path | str] = None,
) -> Path:
    """
    Ensure the directory for a datasource exists and return it.

    This is useful for callers who want to make sure the folder exists
    before an I/O write (handled by io/).

    Example:
        dir_path = ensure_candles_dir("drift")
    """
    path = datasource_dir(datasource, user_root=user_root)
    ensure_dir(path)
    return path
