"""
Generate synthetic 1-second OHLCV candles for benchmarking (deterministic).

Performance depends on row count and column dtypes, not on realistic market
structure, so synthetic candles are sufficient for a pandas-vs-polars *speed*
comparison — no production data required.

Usage:
    python -m benchmarks.gen_data --rows 100k,1m,5m --out benchmarks/data
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

STEP_MS = 1_000  # 1-second candles
BASE_MS = 1_600_000_000_000


def generate(n_rows: int, *, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    open_time = BASE_MS + np.arange(n_rows, dtype=np.int64) * STEP_MS
    close = 100.0 + rng.normal(0.0, 0.5, size=n_rows).cumsum()
    open_ = np.empty(n_rows)
    open_[0] = 100.0
    open_[1:] = close[:-1]
    high = np.maximum(open_, close) + rng.random(n_rows) * 0.5
    low = np.minimum(open_, close) - rng.random(n_rows) * 0.5
    volume = rng.random(n_rows) * 1000.0
    return pd.DataFrame(
        {
            "open_time": open_time,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def parse_size(tok: str) -> int:
    s = tok.strip().lower().replace("_", "")
    if s.endswith("k"):
        return int(float(s[:-1]) * 1_000)
    if s.endswith("m"):
        return int(float(s[:-1]) * 1_000_000)
    return int(float(s))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", required=True, help="comma list, e.g. 100k,1m,5m")
    ap.add_argument("--out", default="benchmarks/data")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for tok in args.rows.split(","):
        n = parse_size(tok)
        path = out / f"candles_1s_{n}.parquet"
        generate(n).to_parquet(path)
        print(f"wrote {path}  ({n:,} rows, {path.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
