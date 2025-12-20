#!/usr/bin/env python
"""
Materializes raw Binance klines into columnar parquet datasets.

This is a long-running process that:
- watches raw manifests
- aggregates slices into parquet parts
- never mutates raw
"""

from __future__ import annotations

import argparse
from pathlib import Path

from __PACKAGE_NAME__.logging_setup import setup_logging, LogProfile
from qlir.data.core.paths import get_data_root
from qlir.data.agg.engine import run_agg_daemon, AggConfig
from qlir.data.agg.paths import DatasetPaths

# Logging is infra-owned (same as data_server)
setup_logging(profile=LogProfile.QLIR_INFO)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--limit", type=int, default=1000)

    parser.add_argument(
        "--batch-slices",
        type=int,
        default=100,
        help="Number of slices per parquet part",
    )

    return parser.parse_args()

def main() -> None:
    args = parse_args()

    data_root = get_data_root()

    raw_root = (
        Path(data_root)
        / "binance"
        / "klines"
        / "raw"
        / args.symbol
        / args.interval
        / f"limit={args.limit}"
    )

    agg_root = (
        Path(data_root)
        / "binance"
        / "klines"
        / "agg"
        / args.symbol
        / args.interval
        / f"limit={args.limit}"
    )

    paths = DatasetPaths(
        raw_root=raw_root,
        agg_root=agg_root,
    )

    dataset_meta = {
        "source": "binance",
        "dataset": "klines",
        "symbol": args.symbol,
        "interval": args.interval,
        "limit": args.limit,
    }

    cfg = AggConfig(batch_slices=args.batch_slices)

    print(
        "[agg_server] starting with\n"
        f"  symbol={args.symbol}\n"
        f"  interval={args.interval}\n"
        f"  limit={args.limit}\n"
        f"  batch_slices={args.batch_slices}\n"
        f"  raw_root={raw_root}\n"
        f"  agg_root={agg_root}"
    )

    run_agg_daemon(
        paths=paths,
        dataset_meta=dataset_meta,
        cfg=cfg,
    )

if __name__ == "__main__":
    main()
