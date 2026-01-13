import argparse
import os
import sys
import subprocess
from time import sleep
import pandas as pd
from qlir.time.timefreq import TimeFreq
from qlir.data.core.instruments import CanonicalInstrument
from afterdata.etl.binance.agg_server import parse_args as parse_agg_server_args
from afterdata.etl.binance.data_server import _add_endpoint_arg, _add_log_profile_arg
from afterdata.logging.logging_setup import setup_logging, LogProfile
import logging

from afterdata.runtime_config import RuntimeConfig
log = logging.getLogger(__name__)

def _fetch_raw_impl(symbols, intervals, endpoint, log_profile) -> None:
    """
    Launch Binance raw data servers. One subproc per symbol/interval/endpoint combo.
    """
    # create a process group once
    pgid = os.getpid()

    os.setpgid(0, pgid)  # launcher becomes group leader

    for symbol in symbols:
        for interval in intervals:
            cmd = [
                sys.executable,
                "-m",
                "afterdata.etl.binance.data_server",
                "--symbol", symbol,
                "--interval", interval,
                "--endpoint", endpoint,
                "--log-profile", log_profile
            ]

            proc = subprocess.Popen(cmd, 
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    preexec_fn=lambda: os.setpgid(0, pgid))
            print(f"[binance] starting {endpoint} {symbol}@{interval} pid={proc.pid}")

def fetch_raw_all() -> None:
    """
    Start raw ingestion for all symbols / intervals.
    """

    raise NotImplementedError("Need to add symbol list endpoint to get all symbols")
    print("[binance] starting raw ingestion (ALL)")
    print(f"  symbols={DEFAULT_SYMBOLS}")
    print(f"  intervals={DEFAULT_INTERVALS}")

    _fetch_raw_impl(
        symbols=DEFAULT_SYMBOLS,
        intervals=DEFAULT_INTERVALS,
    )


def fetch_raw_specific() -> None:
    """
    Start raw ingestion for user-specified symbols / intervals for a specific endpoint
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbols (e.g. BTCUSDT,ETHUSDT)",
    )
    parser.add_argument(
        "--intervals",
        default="1s",
        help="Comma-separated intervals (default: 1s)",
    )

    _add_endpoint_arg(parser)
    _add_log_profile_arg(parser)

    args = parser.parse_args()
    
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]

    print("[binance] starting raw ingestion (SPECIFIC)")
    print(f"  symbols={symbols}")
    print(f"  intervals={intervals}")

    _fetch_raw_impl(
        symbols=symbols,
        intervals=intervals,
        endpoint=args.endpoint,
        log_profile=args.log_profile
    )


def aggregate_raw_specific():
    """
    Start agg server for user-specified symbols / intervals for a specific endpoint
    """
    parser = argparse.ArgumentParser()
    parser = parse_agg_server_args(parser)
    args = parser.parse_args()
    print(f"Args received by lte.main.py {args}")
    print("main.py call agg_server.py")
    subprocess.run(
        [sys.executable, "-m", "afterdata.etl.binance.agg_server", 
         "--endpoint", args.endpoint,
          "--symbol", args.symbol,
          "--interval", args.interval,
          "--limit", str(args.limit),
          "--batch-slices", str(args.batch_slices)
         ],
    )


def normalize(raw: pd.DataFrame) -> pd.DataFrame:
    """Load agg parquet files from this venue/source into canonical candle schema."""
    raise NotImplementedError("Normlization not yet implemented")

# def validate_clean(clean: pd.DataFrame, base_resolution: TimeFreq) -> tuple[pd.DataFrame, CandlesDQReport | None]:
#     """Run shared validation, returns (possibly adjusted df, report)."""
#     raise NotImplementedError("Validation not yet implemented")

def write_clean(clean: pd.DataFrame, symbol: CanonicalInstrument, base_resolution: TimeFreq) -> None:
    """Canonical path + metadata write."""
    raise NotImplementedError("Clean data write not yet implemented")