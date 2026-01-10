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

from afterdata.logging.logging_setup import setup_logging, LogProfile
from qlir.data.core.paths import get_data_root
from qlir.data.agg.engine import run_agg_daemon, AggConfig
from qlir.data.agg.paths import DatasetPaths

# Logging is infra-owned (same as data_server)
setup_logging(profile=LogProfile.QLIR_DEBUG)

def parse_args(parser: argparse.ArgumentParser):

    parser = argparse.ArgumentParser(
        description=(
        "Aggregate raw response data into columnar datasets.\n\n"
        "The aggregation server reads raw API responses previously "
        "persisted on disk, and compacts them into chunked Parquet files"
        "No validation is done here. Check data quality after reading into memory.\n\n"
        "Each server is isolated to one endpoint/symbol/interval/limit combo (just like the data server)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--endpoint",
        choices=["klines", "uiklines"],
        required=True,
        help="The endpoint name [klines, uiklines]"
    )

    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Single Symbol [BTCUSDT, SOLUSDT, etc.]",
    )

    parser.add_argument(
        "--interval",
        required=True,
        help="interval [1s, 1m]",
    )

    parser.add_argument(
        "--limit",
        default=1000,
        type=int,
        required=True,
        help="Response size (used to find the correct data directory)",
    )

    parser.add_argument(
        "--batch-slices",
        type=int,
        default=100,
        help="Number of slices per parquet part",
    )

    return parser


def main() -> None:
    print("main called")
    parser = argparse.ArgumentParser()
    parser = parse_args(parser)
    args = parser.parse_args()

    data_root = get_data_root()
    
    raw_root = (
        Path(data_root)
        / "binance"
        / args.endpoint
        / "raw"
        / args.symbol
        / args.interval
        / f"limit={args.limit}"
    )

    agg_root = (
        Path(data_root)
        / "binance"
        / args.endpoint
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
        "dataset": args.endpoint,
        "symbol": args.symbol,
        "interval": args.interval,
        "limit": args.limit,
    }

    cfg = AggConfig(batch_slices=args['batch-slices'])

    print(
        "[agg_server] starting with\n"
        f"  symbol={args.endpoint}\n"
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
