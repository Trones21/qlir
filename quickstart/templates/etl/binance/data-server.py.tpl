#!/usr/bin/env python
"""
This fetches data from binance
It blocks forever (or as long as the worker loop runs) until killed
by the coordinator process.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from qlir.data.core.paths import get_data_root
from qlir.data.sources.binance.server import (
    BinanceServerConfig,
    KlinesJobConfig,
    start_data_server,
)

def parse_csv_arg(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]

# if you want to limit the data to fetch or maybe store raw data somewhere non-canonical (not recommended, but the option is here )
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",  
        help="Directory where raw Binance data will be written.",
    )

    parser.add_argument(
        "--symbols",
        type=parse_csv_arg,
        default=["BTCUSDT"],
        help="Comma-separated list of symbols (default: BTCUSDT).",
    )

    parser.add_argument(
        "--intervals",
        type=parse_csv_arg,
        default=["1s"],
        help="Comma-separated list of kline intervals (default: 1s).",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Kline limit per request (default: 500).",
    )

    return parser.parse_args()

def main() -> None:
    args = parse_args()

    if args.data_root:
        data_root = Path(args.data_root)
    else:
        data_root = get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)

    klines_jobs = [
        KlinesJobConfig(
            symbol=symbol,
            interval=interval,
            limit=args.limit,
        )
        for symbol in args.symbols
        for interval in args.intervals
    ]

    cfg = BinanceServerConfig(
        klines_jobs=klines_jobs,
        data_root=str(data_root),
        use_threads=False,  # block this process in the worker loop
    )

    print(
        "[data_server] starting with\n"
        f"  symbols={args.symbols}\n"
        f"  intervals={args.intervals}\n"
        f"  limit={args.limit}\n"
        f"  data_root={data_root}"
    )

    start_data_server(cfg)

if __name__ == "__main__":
    main()
