# Defines how to read a raw IBKR historical-bars response and turn it into a table-like object.
# Sibling of schema_binance_klines.py; same contract (read {"data": rows} -> DataFrame with an
# "open_time" column so the agg engine can sort/seal parts uniformly).

from __future__ import annotations

import json
from pathlib import Path

import pandas as _pd

# Row shape written by the IBKR persist layer
# (qlir.data.sources.interactive_brokers.endpoints.historical_bars.persist / .model.BAR_COLUMNS):
#   [open_time_ms, open, high, low, close, volume, average, bar_count]
IBKR_BAR_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "average",
    "bar_count",
]


def load_ibkr_bar_slice_json(path: Path) -> _pd.DataFrame:
    """
    IBKR historical-bars response file: {"meta": {...}, "data": [[...], ...]}
    where each row is the 8-element bar array above.
    """
    with path.open("r", encoding="utf-8") as f:
        full_file = json.load(f)
        rows = full_file["data"]

    if not isinstance(rows, list):
        raise ValueError(f"expected list, got {type(rows).__name__}")

    df = _pd.DataFrame(rows, columns=IBKR_BAR_COLUMNS)

    # Mechanical coercions only (no semantic cleanup) — mirrors the Binance schema.
    if not df.empty:
        df["open_time"] = df["open_time"].astype("int64")
        df["bar_count"] = df["bar_count"].astype("int64")
        for c in ["open", "high", "low", "close", "volume", "average"]:
            df[c] = df[c].astype("float64")

    return df
