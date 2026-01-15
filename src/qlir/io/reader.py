from __future__ import annotations

import logging
from pathlib import Path

import pandas as _pd

log = logging.getLogger(__name__)

def _as_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


# ---------- Explicit readers ----------
def read_csv(path: str | Path, **kwargs) -> _pd.DataFrame:
    """Read CSV into DataFrame. Extra kwargs forwarded to pandas.read_csv."""
    return _pd.read_csv(_as_path(path), **kwargs)


def read_parquet(path: str | Path, **kwargs) -> _pd.DataFrame:
    """Read Parquet into DataFrame. Extra kwargs forwarded to pandas.read_parquet."""
    return _pd.read_parquet(_as_path(path), **kwargs)


def read_json(path: str | Path, **kwargs) -> _pd.DataFrame:
    """
    Read JSON into DataFrame.
    Common kwargs:
      - orient (e.g., "records", "split")
      - lines (True for JSONL)
      - dtype, convert_dates, etc.
    """
    return _pd.read_json(_as_path(path), **kwargs)


# ---------- Dispatcher ----------
def read(path: str | Path, **kwargs) -> _pd.DataFrame:
    """
    Dispatch by file extension:
      .csv      -> read_csv
      .parquet  -> read_parquet
      .json     -> read_json
    Extra kwargs are forwarded to the specific reader.
    """
    p = _as_path(path)

    # --- Safety check: path must be a file, not a directory ---
    if p.is_dir():
        raise ValueError(f"Expected a file, got directory instead: {p}")

    if not p.is_file():
        raise FileNotFoundError(f"No file found at: {p}")
    # -----------------------------------------------------------

    suf = p.suffix.lower()
    if suf == ".csv":
        return read_csv(p, **kwargs)
    if suf == ".parquet":
        return read_parquet(p, **kwargs)
    if suf == ".json":
        return read_json(p, **kwargs)
    # Added so that we can keep static files for our tests, yet leave gitignore the same
    if suf == ".test_csv":
        return read_csv(p, **kwargs)
    
    raise ValueError(f"""
                     Unsupported extension: {suf} 
                     Use .csv, .parquet, or .json.
                     """)


def load_latest_parquet_window(
    directory: Path,
    *,
    pattern: str = "*.parquet",
    window_size: int,
    head_name: str = "head.parquet",
) -> _pd.DataFrame:
    """
    Load a rolling window of paquet files consisting of:
      - the latest (window_size - 1) chunk files
      - plus head.parquet (if present)

    window_size >= 1

    Returns a raw, unordered, non-deduplicated staging DataFrame.
    ETL MUST be applied before analytical use.
    """
    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    all_files = sorted(directory.glob(pattern))
    if not all_files:
        return _pd.DataFrame()

    head_path = directory / head_name
    has_head = head_path.exists()

    # Exclude head from chunk candidates
    chunk_files = [
        p for p in all_files
        if p.name != head_name
    ]

    # Take last (window_size - 1) chunks
    n_chunks = window_size - 1
    selected_chunks = chunk_files[-n_chunks:] if n_chunks > 0 else []

    selected_files = selected_chunks.copy()
    if has_head:
        selected_files.append(head_path)

    if not selected_files:
        return _pd.DataFrame()

    log.info("Loading parquet window: %s", selected_files)

    dfs = [_pd.read_parquet(p) for p in selected_files]
    return _pd.concat(dfs, ignore_index=True)






















# def read_candles(
#     path: str | Path,
#     *,
#     fill: FillMode = "none",            # "none" or "fetch"
#     symbol: Optional[str] = None,       # required if fill="fetch"
#     token: Optional[str] = None,        # Drift token; improves gap detection
#     fetch_kwargs: Optional[Dict[str, Any]] = None,  # passed to backfill_gaps_drift
#     **read_kwargs,
# ) -> _pd.DataFrame:
#     """
#     Read candles from CSV/Parquet/JSON, validate, and optionally fetch-fill real gaps from Drift.
#     """
#     df = read(path, **read_kwargs)
#     if df.empty:
#         return df

#     fixed, report = validate_candles(df, token=token)
#     print(
#         f"[candles_dq] file={Path(path).name} rows={report.n_rows} "
#         f"dupes_dropped={report.n_dupes_dropped} gaps={report.n_gaps} "
#         f"freq={report.freq or 'unknown'}"
#     )

#     if fill == "none" or report.n_gaps == 0:
#         return fixed

#     if fill == "fetch":
#         if not symbol or not token:
#             raise ValueError("read_candles(fill='fetch') requires symbol= and token= (Drift token).")
#         filled = backfill_gaps_drift(
#             fixed, symbol=symbol, token=token, **(fetch_kwargs or {})
#         )
#         return filled

#     raise ValueError(f"Unknown fill='{fill}'")