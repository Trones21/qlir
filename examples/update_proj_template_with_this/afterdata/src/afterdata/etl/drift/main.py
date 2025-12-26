import sys
import subprocess

def fetch_raw(...) -> pd.DataFrame:
    """Source-specific. No cleaning, no validation, no writing."""
    subprocess.run([sys.executable, "fetch_initial_data.py", check=True])

def normalize_raw(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert raw from this venue/source into canonical candle schema."""

def validate_clean(clean: pd.DataFrame, base_resolution: TimeFreq) -> tuple[pd.DataFrame, DQReport]:
    """Run shared validation, returns (possibly adjusted df, report)."""

def write_clean(clean: pd.DataFrame, symbol: CanonicalInstrument, base_resolution: TimeFreq) -> None:
    """Canonical path + metadata write."""
