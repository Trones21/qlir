#!/usr/bin/env python
"""
This fetches data from binance
It blocks forever (or as long as the worker loop runs) until killed
by the coordinator process.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import string
from qlir.data.core.paths import get_data_root
from afterdata.logging.logging_setup import setup_logging, LogProfile
from qlir.data.sources.binance.server import (
    WorkerType,
    KlinesServerConfig,
    UIKlinesServerConfig,
    KlinesJobConfig,
    UIKlinesJobConfig,
    start_data_server
)

from afterdata.runtime_config import RuntimeConfig

def parse_csv_arg(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]

def _add_endpoint_arg(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--endpoint",
        required=True,
        choices=["klines", "uiklines"],
        help="Endpoint [klines, uklines]",
    )

def _add_log_profile_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-profile",
        type=LogProfile,
        choices=list(LogProfile),
        default=LogProfile.QLIR_INFO,
        help="Logging profile",
    )


# if you want to limit the data to fetch or maybe store raw data somewhere non-canonical (not recommended, but the option is here )
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",  
        help="Directory where raw Binance data will be written.",
    )

    parser.add_argument(
        "--symbol",
        type=str,
        help="Single Symbol [BTCUSDT, SOLUSDT, etc.]",
    )

    parser.add_argument(
        "--interval",
        type=str,
        help="interval [1s, 1m]",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Kline/uikline limit per request (default: 1000).",
    )

    _add_endpoint_arg(parser)
    _add_log_profile_arg(parser)
    
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    if args.data_root:
        data_root = Path(args.data_root)
    else:
        data_root = get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)

    # What we want to run 
    data_server_cfg = None

    if args.endpoint == 'klines':
        klines_server_cfg = KlinesServerConfig(
            data_root=data_root,
            worker_type=WorkerType.KLINES,
            klines=KlinesJobConfig(
            symbol=args.symbol,
            interval=args.interval,
            limit=args.limit,
        ))

        data_server_cfg = klines_server_cfg

    if args.endpoint == 'uiklines':
        uiklines_server_cfg = UIKlinesServerConfig(
            data_root=data_root,
            worker_type=WorkerType.UI_KLINES,
            ui_klines=UIKlinesJobConfig(
            symbol=args.symbol,
            interval=args.interval,
            limit=args.limit,
        ))

        data_server_cfg = uiklines_server_cfg
    
    if data_server_cfg is None:
        raise SystemError("No matching config was found, check the args that you passed")


    # How we want to run it
    runtime_cfg = RuntimeConfig(log_profile=args.log_profile)
    setup_logging(profile=runtime_cfg.log_profile)

    # Start the Data Server
    print(
        "[data_server] starting with\n"
        f"  symbol={args.symbol}\n"
        f"  interval={args.interval}\n"
        f"  limit={args.limit}\n"
        f"  data_root={data_root}"
    )

    start_data_server(data_server_cfg)

if __name__ == "__main__":
    main()
