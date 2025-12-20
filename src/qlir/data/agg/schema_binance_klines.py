# Defines how to read a raw response and turn it into a table-like object.
# todo: swap pandas to pyarrow.Table later.

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

BINANCE_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "num_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

def load_binance_kline_slice_json(path: Path) -> pd.DataFrame:
    """
    Binance /api/v3/klines response: list[list]
    Each row is the 12-element kline array.
    """
    with path.open("r", encoding="utf-8") as f:
        rows = json.load(f)

    if not isinstance(rows, list):
        raise ValueError(f"expected list, got {type(rows).__name__}")

    df = pd.DataFrame(rows, columns=BINANCE_KLINE_COLUMNS)

    # Mechanical coercions only (no semantic cleanup)
    df["open_time"] = df["open_time"].astype("int64")
    df["close_time"] = df["close_time"].astype("int64")
    df["num_trades"] = df["num_trades"].astype("int64")

    # floats as float64 (binance returns strings for many)
    float_cols = [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    for c in float_cols:
        df[c] = df[c].astype("float64")

    # 'ignore' can remain object or be dropped; keep it for fidelity
    return df
