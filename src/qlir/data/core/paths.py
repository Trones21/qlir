# data/core/paths.py
from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Canonical dataset path builders
# ---------------------------------------------------------------------------

def candles_path(
    *,
    instrument_id: str,
    resolution: str,
    datasource: str,
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
    resolution : str
        Canonical resolution string ("1m", "5m", "1h", ...)
    datasource : str
        Name of the datasource (e.g. "drift", "helius", "kaiko")
    user_root : Path | str | None
        Overrides the data root. If None, `get_data_root` is used.
    ext : str
        File extension (default: ".parquet")

    Returns
    -------
    Path
        Full canonical path to the dataset file.
    """
    ds_dir = datasource_dir(datasource, user_root=user_root)
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
