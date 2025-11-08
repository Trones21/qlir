from __future__ import annotations
import json
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


def write_dataset_meta(dataset_path: str | Path, *, symbol: str, resolution: str, **extra: Any) -> None:
    """
    Write a sidecar .meta.json next to the dataset.

    dataset_path: path to the main data file (csv/parquet/json)
    symbol: e.g. "SOL-PERP"
    resolution: e.g. "1m"
    extra: any additional fields you want to persist (venue="drift", source="api", etc.)
    """
    if not isinstance(dataset_path, Path):
        dataset_path = Path(dataset_path)

    meta_path = dataset_path.with_suffix(".meta.json")

    meta = {
        "symbol": symbol,
        "resolution": resolution,
        **extra,
    }

    # pretty-print so you can debug with your eyes
    meta_path.write_text(json.dumps(meta, indent=2))
