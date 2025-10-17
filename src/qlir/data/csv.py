from __future__ import annotations
import os
import pandas as pd

REQUIRED = ["timestamp", "open", "high", "low", "close", "volume"]

def load_ohlcv_from_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    df = pd.read_csv(path)

    # Map common header variants to canonical names
    lower = {c.lower(): c for c in df.columns}
    aliases = {
        "timestamp": ["timestamp", "time", "date"],
        "open":      ["open", "o"],
        "high":      ["high", "h"],
        "low":       ["low", "l"],
        "close":     ["close", "c"],
        "volume":    ["volume", "vol", "basevolume", "qty"],
    }
    rename = {}
    for want, alts in aliases.items():
        hit = next((lower[a] for a in alts if a in lower), None)
        if not hit:
            raise ValueError(f"Missing required column: {want}")
        rename[hit] = want

    df = df.rename(columns=rename)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    num_cols = ["open", "high", "low", "close", "volume"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (df
          .dropna(subset=["timestamp", *num_cols])
          .sort_values("timestamp")
          .reset_index(drop=True))
    return df
