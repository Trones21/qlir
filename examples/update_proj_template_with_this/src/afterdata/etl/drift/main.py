import sys
import subprocess

def fetch_raw(...) -> _pd.DataFrame:
    """Source-specific. No cleaning, no validation, no writing."""
    subprocess.run([sys.executable, "fetch_initial_data.py", check=True])

def normalize_raw(raw: _pd.DataFrame) -> _pd.DataFrame:
    """Convert raw from this venue/source into canonical candle schema."""

def validate_clean(clean: _pd.DataFrame, base_resolution: TimeFreq) -> tuple[_pd.DataFrame, DQReport]:
    """Run shared validation, returns (possibly adjusted df, report)."""

def write_clean(clean: _pd.DataFrame, symbol: CanonicalInstrument, base_resolution: TimeFreq) -> None:
    """Canonical path + metadata write."""
