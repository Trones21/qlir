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

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_DEBUG)

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
        "--endpoint",
        choices=["klines", "uiklines"],
        help="The endpoint name [klines, uiklines]"
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

    return parser.parse_args()

def main() -> None:
    args = parse_args()

    if args.data_root:
        data_root = Path(args.data_root)
    else:
        data_root = get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)

    cfg = None

    if args.endpoint == 'klines':
        klines_server_cfg = KlinesServerConfig(
            data_root=data_root,
            worker_type=WorkerType.KLINES,
            klines=KlinesJobConfig(
            symbol=args.symbol,
            interval=args.interval,
            limit=args.limit,
        ))

        cfg = klines_server_cfg

    if args.endpoint == 'uiklines':
        uiklines_server_cfg = UIKlinesServerConfig(
            data_root=data_root,
            worker_type=WorkerType.UI_KLINES,
            ui_klines=UIKlinesJobConfig(
            symbol=args.symbol,
            interval=args.interval,
            limit=args.limit,
        ))

        cfg = uiklines_server_cfg
    
    if cfg is None:
        raise SystemError("No matching config was found, check the args that you passed")

    print(
        "[data_server] starting with\n"
        f"  symbol={args.symbol}\n"
        f"  interval={args.interval}\n"
        f"  limit={args.limit}\n"
        f"  data_root={data_root}"
    )

    start_data_server(cfg)

if __name__ == "__main__":
    main()
