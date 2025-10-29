from __future__ import annotations
from pathlib import Path
import pandas as pd


def _prep_path(path: str | Path) -> Path:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    return p.resolve()


# ---------- Explicit writers ----------
def write_csv(df: pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """Write DataFrame to CSV (overwrite). Extra kwargs forwarded to DataFrame.to_csv."""
    path = _prep_path(path)
    df.to_csv(path, index=False, **kwargs)
    print(f"Wrote csv to {path}")
    return path


def write_parquet(df: pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """Write DataFrame to Parquet (overwrite). Extra kwargs forwarded to DataFrame.to_parquet."""
    path = _prep_path(path)
    df.to_parquet(path, index=False, **kwargs)
    print(f"Wrote parquet to {path}")
    return path


def write_json(df: pd.DataFrame, path: str | Path, **kwargs) -> Path:
    """
    Write DataFrame to JSON (overwrite).
    Common kwargs: orient="records", indent=2, lines=False, date_format="iso"
    """
    path = _prep_path(path)
    df.to_json(path, **kwargs)
    print(f"Wrote json to {path}")
    return path


# ---------- Dispatcher ----------
def write(df: pd.DataFrame, path: str | Path, **kwargs) -> Path:
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
