from pathlib import Path
import sys
import subprocess
import pandas as pd
from qlir.time.timefreq import TimeFreq
from qlir.data.core.instruments import CanonicalInstrument
from __PACKAGE_NAME__.logging_setup import setup_logging, LogProfile
import logging
log = logging.getLogger(__name__)

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_INFO)

def fetch_raw():
    """Source-specific. No cleaning, no validation"""

    # This starts a single server that fetches multiple symbols and intervals
    args = ["--symbol", "BTCUSDT,ETHUSDT,SOLUSDT", "--interval", "1m" ]
    cmd = [
        sys.executable,
        "-m",
        "__PACKAGE_NAME__.etl.binance.data_server",
        *args
    ]
    subprocess.run(cmd, check=True)


def aggregate_raw():
    args = ["--symbol", "BTCUSDT", "--interval", "1m", "--limit", "1000"]
    subprocess.run(
        [sys.executable, "-m", "__PACKAGE_NAME__.etl.binance.agg_server", *args],
        check=True,
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