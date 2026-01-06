from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Optional
import pandas as _pd

from qlir.data.core.naming import sidecar_metadata


def _prep_path(path: str | Path) -> Path:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    return p.resolve()


# ---------- Explicit writers ----------
def write_csv(df: _pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """Write DataFrame to CSV (overwrite). Extra kwargs forwarded to DataFrame.to_csv."""
    path = _prep_path(path)
    df.to_csv(path, index=False, **kwargs)
    print(f"Wrote csv to {path}")
    return path


def write_parquet(df: _pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """Write DataFrame to Parquet (overwrite). Extra kwargs forwarded to DataFrame.to_parquet."""
    path = _prep_path(path)
    df.to_parquet(path, index=True, **kwargs)
    print(f"Wrote parquet to {path}")
    return path


def write_json(df: _pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """
    Write DataFrame to JSON (overwrite).
    Common kwargs: orient="records", indent=2, lines=False, date_format="iso"
    """
    path = _prep_path(path)
    df.to_json(path, **kwargs)
    print(f"Wrote json to {path}")
    return path


# ---------- Dispatcher ----------
def write(df: _pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """
    Dispatch by file extension:
      .csv      -> write_csv
      .parquet  -> write_parquet
      .json     -> write_json
    Extra kwargs are forwarded to the specific writer.
    """
    path = _prep_path(path)
    suf = path.suffix.lower()
    if suf == ".csv":
        return write_csv(df, path, **kwargs)
    if suf == ".parquet":
        return write_parquet(df, path, **kwargs)
    if suf == ".json":
        return write_json(df, path, **kwargs)
    raise ValueError(f"Unsupported extension {suf!r}. Use .csv, .parquet, or .json.")


def write_dataset_meta(
    dataset_path: str | Path,
    *,
    instrument_id: str,
    resolution: str,
    datasource: Optional[str] = None,
    upstream_symbol: Optional[str] = None,
    qlir_version: Optional[str] = None,
    **extra: Any,
) -> None:
    """
    Write a canonical sidecar .meta.json file next to a dataset.

    Parameters
    ----------
    dataset_path : str | Path
        Path to the primary dataset file (e.g. .parquet, .csv, .json).
    instrument_id : str
        Canonical instrument ID (e.g. "sol-perp", "btc-perp").
        This is the only required naming field.
    resolution : str
        Canonical resolution string (e.g. "1m", "5m", "1h", "1D").
    datasource : str, optional
        Name of the datasource that produced the dataset ("drift", "helius", ...).
    upstream_symbol : str, optional
        Venue-specific symbol used to fetch the data ("SOL-PERP", "SOL/USDC").
    qlir_version : str, optional
        Version tag for the QLIR data layout (optional future-proofing).
    extra : Any
        Additional key/value pairs that will be included in the metadata.

    Notes
    -----
    - The resulting JSON always follows the canonical schema defined in
      `data.core.naming.sidecar_metadata`.
    - This function does *not* write parquet metadata — only the sidecar JSON.
    """
    if not isinstance(dataset_path, Path):
        dataset_path = Path(dataset_path)

    meta_path = dataset_path.with_suffix(".meta.json")

    # Build metadata using canonical schema
    meta_dict = sidecar_metadata(
        instrument_id=instrument_id,
        resolution=resolution,
        datasource=datasource,
        upstream_symbol=upstream_symbol,
        qlir_version=qlir_version,
        extra=extra or None,
    )

    # Write pretty JSON (easier for debugging)
    meta_path.write_text(json.dumps(meta_dict, indent=2))
    print(f"Wrote sidecar metadata to {meta_path}")