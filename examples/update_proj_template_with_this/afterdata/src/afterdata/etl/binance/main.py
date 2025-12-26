import argparse
import os
import sys
import subprocess
from time import sleep
import pandas as pd
from qlir.time.timefreq import TimeFreq
from qlir.data.core.instruments import CanonicalInstrument
from afterdata.etl.binance.agg_server import parse_args as parse_agg_server_args
from afterdata.logging.logging_setup import setup_logging, LogProfile
import logging
log = logging.getLogger(__name__)

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_DEBUG)

def _get_endpoint_arg(parser: argparse.ArgumentParser) -> str:
    parser.add_argument(
        "--endpoint",
        required=True,
        help="Endpoint [klines, uklines]",
    )

    args = parser.parse_args()
    return args.endpoint


def _fetch_raw_impl(symbols, intervals, endpoint) -> None:
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
                "--endpoint", endpoint
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


DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_INTERVALS = ["1m"]

def fetch_raw_default() -> None:
    parser = argparse.ArgumentParser()
    endpoint = _get_endpoint_arg(parser)
    print(
        "[NOTE] This command spawns long-running worker processes.\n"
        "Ctrl+C will NOT stop the servers.\n"
        "You must terminate these by PID (kill / pkill).\n"
        "Logging.prepend_pid is being set to true for your convenience \n"
        "You can also find the pids with: \n"
        "   ps aux | grep -E 'binance.data_server|^USER' \n"
        "To stop a single/multiple worker:\n"
        "   kill <pid found> <pid found> etc. \n"
        "To stop all workers:\n"
        "   pkill -f binance.data_server\n"
    )
    print(f"[Note] Printing to the console will be interwoven. \n")
    print(f"[binance] starting raw ingestion of symbols:  \n {DEFAULT_SYMBOLS} \n at intervals: \n {DEFAULT_INTERVALS} \n\n")
    print(f" Servers starting in 10 seconds...")
    sleep(10)

    _fetch_raw_impl(DEFAULT_SYMBOLS, DEFAULT_INTERVALS, endpoint)


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

    endpoint = _get_endpoint_arg(parser)
    poetry_args = parser.parse_args()
    
    symbols = [s.strip() for s in poetry_args.symbols.split(",") if s.strip()]
    intervals = [i.strip() for i in poetry_args.intervals.split(",") if i.strip()]

    print("[binance] starting raw ingestion (SPECIFIC)")
    print(f"  symbols={symbols}")
    print(f"  intervals={intervals}")

    _fetch_raw_impl(
        symbols=symbols,
        intervals=intervals,
        endpoint=endpoint
    )


def aggregate_raw_specific():
    """
    Start agg server for user-specified symbols / intervals for a specific endpoint
    """
    parser = argparse.ArgumentParser()
    parser = parse_agg_server_args(parser)
    args = parser.parse_args()
    subprocess.run(
        [sys.executable, "-m", "afterdata.etl.binance.agg_server", 
         "--endpoint", args.endpoint,
          "--symbol", args.symbol,
          "--interval", args.interval,
          "--limit", str(args.limit)
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