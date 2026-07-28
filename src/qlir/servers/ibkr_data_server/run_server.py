#!/usr/bin/env python
"""
Interactive Brokers data server entrypoint.

Fetches historical bars from a running IB Gateway / TWS and persists raw slices to disk,
mirroring the Binance data_server. Blocks forever (supervised) until killed.

PREREQUISITE: a running, logged-in IB Gateway or TWS reachable at --host/--port. The pipeline
must run OUTSIDE the US only if you also run the Binance data server; IBKR itself has no such
geo restriction, but it DOES require the Gateway/TWS app and the relevant market-data
entitlements for the instrument.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from qlir.data.core.paths import get_data_root
from qlir.data.sources.interactive_brokers import bar_sizes
from qlir.data.sources.interactive_brokers.job_config_models import HistoricalBarsJobConfig
from qlir.data.sources.interactive_brokers.server import start_data_server
from qlir.data.sources.interactive_brokers.server_config_models import (
    HistoricalBarsServerConfig,
    WorkerType,
)
from qlir.servers.data_server.runtime_config import RuntimeConfig
from qlir.servers.logging.logging_setup import LogProfile, setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest Interactive Brokers historical bars into raw slices on disk.\n"
            "Requires a running, logged-in IB Gateway/TWS. Phase 1 supports sec_type=STK."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--symbol", type=str, required=True, help="Ticker, e.g. AAPL")
    parser.add_argument("--sec-type", type=str, default="STK", choices=["STK"], help="Phase 1: STK only")
    parser.add_argument("--exchange", type=str, default="SMART")
    parser.add_argument("--currency", type=str, default="USD")
    parser.add_argument("--primary-exchange", type=str, default=None, help="Optional, e.g. NASDAQ")

    parser.add_argument(
        "--bar-size",
        type=str,
        required=True,
        help=f"Bar size token. Supported: {bar_sizes.supported_tokens()}",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Bars per slice (default 1000)")
    parser.add_argument("--what-to-show", type=str, default="TRADES")
    parser.add_argument("--use-rth", type=int, default=0, choices=[0, 1], help="0=all hours, 1=RTH only")

    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002, help="4002 paper / 4001 live gateway; 7497/7496 TWS")
    parser.add_argument("--client-id", type=int, default=1)

    parser.add_argument("--data-root", help="Directory where raw IBKR data will be written.")
    parser.add_argument(
        "--log-profile",
        type=LogProfile,
        choices=list(LogProfile),
        default=LogProfile.QLIR_INFO,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)

    # Normalize the bar-size token up front so the manifest path (used by BOTH the Fetcher
    # and the Manifest Builder) is the canonical token, not an alias like "1h".
    canonical_interval = bar_sizes.normalize_interval(args.bar_size)

    job_config = HistoricalBarsJobConfig(
        symbol=args.symbol,
        interval=canonical_interval,
        limit=args.limit,
        sec_type=args.sec_type,
        exchange=args.exchange,
        currency=args.currency,
        primary_exchange=args.primary_exchange,
        what_to_show=args.what_to_show,
        use_rth=args.use_rth,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
    )

    server_cfg = HistoricalBarsServerConfig(
        data_root=data_root,
        worker_type=WorkerType.HISTORICAL_BARS,
        job_config=job_config,
    )

    runtime_cfg = RuntimeConfig(log_profile=args.log_profile)
    setup_logging(profile=runtime_cfg.log_profile)

    print(
        "[ibkr_data_server] starting with\n"
        f"  symbol={args.symbol} ({args.sec_type}/{args.exchange}/{args.currency})\n"
        f"  bar_size={canonical_interval} -> {bar_sizes.ibkr_bar_size(canonical_interval)}\n"
        f"  limit={args.limit}  what_to_show={args.what_to_show}  use_rth={args.use_rth}\n"
        f"  gateway={args.host}:{args.port} clientId={args.client_id}\n"
        f"  data_root={data_root}"
    )

    start_data_server(server_cfg)


if __name__ == "__main__":
    main()
