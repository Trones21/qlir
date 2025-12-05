#!/usr/bin/env python
"""
Smoke-test data server process.

This runs the *real* Binance data server against a given data_root.
It blocks forever (or as long as the worker loop runs) until killed
by the coordinator process.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from qlir.data.sources.binance.server import (
    BinanceServerConfig,
    KlinesJobConfig,
    start_data_server,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-root",
        required=True,
        help="Directory where raw Binance data will be written.",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Symbol to ingest (default: BTCUSDT).",
    )
    parser.add_argument(
        "--interval",
        default="1s",
        help="Kline interval (default: 1s).",
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

    data_root = Path(args.data_root)
    data_root.mkdir(parents=True, exist_ok=True)

    cfg = BinanceServerConfig(
        klines_jobs=[
            KlinesJobConfig(
                symbol=args.symbol,
                interval=args.interval,
                limit=args.limit,
            )
        ],
        data_root=str(data_root),
        use_threads=False,  # block this process in the worker loop
    )

    print(
        f"[data_server] starting with "
        f"symbol={args.symbol} interval={args.interval} "
        f"limit={args.limit} data_root={data_root}"
    )

    # This should not return under normal circumstances.
    start_data_server(cfg)


if __name__ == "__main__":
    main()
