from __future__ import annotations
from pathlib import Path
import pandas as pd


def _as_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


# ---------- Explicit readers ----------
def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    """Read CSV into DataFrame. Extra kwargs forwarded to pandas.read_csv."""
    return pd.read_csv(_as_path(path), **kwargs)


def read_parquet(path: str | Path, **kwargs) -> pd.DataFrame:
    """Read Parquet into DataFrame. Extra kwargs forwarded to pandas.read_parquet."""
    return pd.read_parquet(_as_path(path), **kwargs)


def read_json(path: str | Path, **kwargs) -> pd.DataFrame:
    """
    Read JSON into DataFrame.
    Common kwargs:
      - orient (e.g., "records", "split")
      - lines (True for JSONL)
      - dtype, convert_dates, etc.
    """
    return pd.read_json(_as_path(path), **kwargs)


# ---------- Dispatcher ----------
def read(path: str | Path, **kwargs) -> pd.DataFrame:
    """
    Dispatch by file extension:
      .csv      -> read_csv
      .parquet  -> read_parquet
      .json     -> read_json
    Extra kwargs are forwarded to the specific reader.
    """
    p = _as_path(path)
    suf = p.suffix.lower()
    if suf == ".csv":
        return read_csv(p, **kwargs)
    if suf == ".parquet":
        return read_parquet(p, **kwargs)
    if suf == ".json":
        return read_json(p, **kwargs)
    raise ValueError(f"Unsupported extension {suf!r}. Use .csv, .parquet, or .json.")
