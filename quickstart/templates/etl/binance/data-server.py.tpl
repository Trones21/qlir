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
from __PACKAGE_NAME__.logging_setup import setup_logging, LogProfile
from qlir.data.sources.binance.server import (
    BinanceServerConfig,
    KlinesJobConfig,
    start_data_server,
)

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_INFO)

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
        "--symbol",
        type=parse_csv_arg,
        default=["BTCUSDT"],
        help="Symbol (default: BTCUSDT).",
    )

    parser.add_argument(
        "--interval",
        type=parse_csv_arg,
        default=["1s"],
        help="kline intervals (default: 1s).",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Kline limit per request (default: 1000).",
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
        for symbol in args.symbol
        for interval in args.interval
    ]

    cfg = BinanceServerConfig(
        klines_jobs=klines_jobs,
        data_root=str(data_root),
        use_threads=False,  # block this process in the worker loop
    )

    if not cfg.use_threads and len(klines_jobs) != 1:
        raise ValueError(
            "Non-threaded mode supports exactly one job per process. "
            "Run one server per symbol/interval."
        )

    print(
        "[data_server] starting with\n"
        f"  symbol={args.symbol}\n"
        f"  intervals={args.interval}\n"
        f"  limit={args.limit}\n"
        f"  data_root={data_root}"
    )

    start_data_server(cfg)

if __name__ == "__main__":
    main()
