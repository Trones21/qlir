# demos/viz_demo.py
"""
External-consumer demo for the QLIR visualization layer.

Data source options:
  1) CLI fetch (real data via QLIR)  -> --source cli
  2) File loader (path to CSV/Parquet/JSON) -> --source file --path <file>
  3) Synthetic (quick demo)           -> --source synth  [default]

Examples:
  python demos/viz_demo.py --source synth --save-dir _out
  python demos/viz_demo.py --source file --path data/ohlcv.csv --save-dir _out
  python demos/viz_demo.py --source cli --market SOL-PERP --resolution 1m --limit 1500 --save-dir _out

Requirements:
  - qlir installed (editable or site package)
  - matplotlib

Outputs:
  - Saves example charts as PNGs to the chosen directory.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import matplotlib

matplotlib.use("Agg")  # headless-safe

import pandas as _pd
import numpy as _np
import matplotlib.pyplot as plt

# --- QLIR imports ---
from qlir.viz import render
from qlir.viz.views.vwap import vwap_distance_view
from qlir.viz.views.boll import boll_validation_view
from qlir.features.vwap.block import add_distance_metrics
from qlir.features.boll.block import with_bollinger
from qlir.indicators.vwap import with_vwap_cum_hlc3
from qlir.io.reader import read  # use built-in reader dispatcher

# Optional data fetcher (CLI-backed real data source)
try:
    from qlir.data.sources.drift.fetch import get_candles_all
except Exception:
    fetch_drift_candles = None

# -----------------------------
# Data sources
# -----------------------------

def make_synth(rows: int, seed: int = 42) -> _pd.DataFrame:
    """Create a synthetic OHLCV dataset for demonstration."""
    rng = _np.random.default_rng(seed)
    idx = _pd.date_range("2025-01-01 06:00", periods=rows, freq="1min")

    close = 100 + _np.cumsum(rng.normal(0.0, 0.2, size=rows))
    high = close + rng.normal(0.1, 0.05, size=rows)
    low = close - rng.normal(0.1, 0.05, size=rows)
    open_ = close + rng.normal(0.0, 0.03, size=rows)
    volume = rng.integers(100, 1000, size=rows)

    df = _pd.DataFrame({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=idx)

    df = with_vwap_cum_hlc3(df)
    return df


def load_file(path: str) -> _pd.DataFrame:
    """Load file via qlir.io.reader.read dispatcher (csv, parquet, json)."""
    df = read(path)
    # Ensure datetime index
    if not isinstance(df.index, _pd.DatetimeIndex):
        if "time" in df.columns:
            df = df.set_index(_pd.to_datetime(df["time"]))
        else:
            raise ValueError("File must have datetime index or 'time' column.")
    # Ensure VWAP exists
    if "vwap" not in df.columns:
        df = with_vwap_cum_hlc3(df)
    return df


def fetch(symbol: str, resolution: str, limit: int) -> _pd.DataFrame:
    if fetch_drift_candles is None:
        raise RuntimeError("CLI fetch not available: qlir.data.drift.fetch_drift_candles not importable")
    df = fetch_drift_candles(symbol=symbol, resolution=resolution, limit=limit)
    if "vwap" not in df.columns:
        df = with_vwap_cum_hlc3(df)
    return df


# -----------------------------
# Rendering helpers
# -----------------------------

def save_figs(figs, outdir: Path, prefix: str):
    outdir.mkdir(parents=True, exist_ok=True)
    for i, fig in enumerate(figs, 1):
        p = outdir / f"{prefix}_{i:02d}.png"
        fig.savefig(p, bbox_inches="tight", dpi=140)
        plt.close(fig)


# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["synth", "file", "cli"], default="synth")
    ap.add_argument("--path", type=str, default=None, help="Path to CSV/Parquet/JSON when --source file")
    ap.add_argument("--market", type=str, default="SOL-PERP", help="Market when --source cli")
    ap.add_argument("--resolution", type=str, default="1m", help="Candle resolution when --source cli")
    ap.add_argument("--limit", type=int, default=1200, help="Max candles when --source cli")

    ap.add_argument("--rows", type=int, default=900, help="Rows when --source synth")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed when --source synth")

    ap.add_argument("--save-dir", type=str, default="_out", help="Where to write demo PNGs")
    args = ap.parse_args()

    if args.source == "file":
        if not args.path:
            raise SystemExit("--path is required when --source file")
        df = load_file(args.path)
    elif args.source == "cli":
        df = fetch(args.market, args.resolution, args.limit)
    else:
        df = make_synth(args.rows, args.seed)

    # Apply QLIR feature blocks
    df = add_distance_metrics(df)
    df = add_bollinger(df)

    # 1) VWAP Distance view
    vw_view = vwap_distance_view()
    vw_figs = render(vw_view, df)
    save_figs(vw_figs, Path(args.save_dir), "vwap_distance")

    # 2) Bollinger Validation view
    bb_view = boll_validation_view()
    bb_figs = render(bb_view, df)
    save_figs(bb_figs, Path(args.save_dir), "boll_validation")

    print(f"Saved {len(vw_figs) + len(bb_figs)} figures -> {args.save_dir}")


if __name__ == "__main__":
    main()
