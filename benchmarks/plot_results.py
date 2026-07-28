"""
Turn benchmarks/results/results.csv into comparison charts.

Charts (PNG -> benchmarks/results/plots/):
  - wall_time.png   median compute time vs rows, per pipeline (log-log)
  - peak_memory.png peak RSS vs rows, per pipeline (log-log)
  - speedup.png     pandas/polars ratio vs rows (>1 = polars faster)

Runs that hit memory pressure (swap / near-OOM) are drawn as red X markers and
excluded from the fitted lines and the speedup — their timings reflect paging,
not engine speed. Uses the latest run per (pipeline, engine, size).

Usage:
    python -m benchmarks.plot_results
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# validated categorical pair (dataviz palette): slot-1 blue, slot-2 orange.
PANDAS, POLARS = "#2a78d6", "#eb6834"
DISTORT = "#d03b3b"  # status:critical — always paired with an X marker + label
INK, SECOND, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
SURFACE, AXIS = "#fcfcfb", "#c3c2b7"
ENGINE_COLOR = {"pandas": PANDAS, "polars": POLARS}

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE, "axes.edgecolor": AXIS,
        "axes.labelcolor": SECOND, "text.color": INK,
        "xtick.color": MUTED, "ytick.color": MUTED, "grid.color": GRID,
        "font.family": "sans-serif", "axes.grid": True, "grid.linewidth": 0.6,
        "axes.spines.top": False, "axes.spines.right": False,
    }
)


def _truthy(x) -> bool:
    return str(x).strip().lower() in ("true", "1", "1.0")


def load(results_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(results_csv)
    # latest run per config
    df = (
        df.sort_values("timestamp")
        .groupby(["pipeline", "engine", "n_rows"], as_index=False)
        .last()
    )
    df["distorted"] = df["mem_pressure"].apply(_truthy) if "mem_pressure" in df else False
    return df


def _dedup_legend(ax) -> None:
    handles, labels = ax.get_legend_handles_labels()
    seen: dict = {}
    for h, l in zip(handles, labels):
        seen.setdefault(l, h)
    ax.legend(seen.values(), seen.keys(), frameon=False, fontsize=9, labelcolor=SECOND)


def _series_panel(ax, sub, ycol, yscale="log"):
    for engine in ("pandas", "polars"):
        e = sub[sub.engine == engine].sort_values("n_rows")
        clean, dist = e[~e.distorted], e[e.distorted]
        ax.plot(clean.n_rows, clean[ycol], "-o", color=ENGINE_COLOR[engine],
                label=engine, lw=2, ms=7)
        if len(dist):
            ax.scatter(dist.n_rows, dist[ycol], marker="x", s=70, color=DISTORT,
                       zorder=6, label="mem pressure (distorted)")
        if len(clean):
            ax.annotate(f" {engine}", (clean.n_rows.iloc[-1], clean[ycol].iloc[-1]),
                        color=ENGINE_COLOR[engine], fontsize=9, va="center")
    ax.set_xscale("log")
    ax.set_yscale(yscale)


def small_multiples(df, ycol, ylabel, title, out_path):
    pipes = sorted(df.pipeline.unique())
    fig, axes = plt.subplots(1, len(pipes), figsize=(5.6 * len(pipes), 4.4), squeeze=False)
    for ax, pipe in zip(axes[0], pipes):
        _series_panel(ax, df[df.pipeline == pipe], ycol)
        ax.set_title(pipe, color=INK, fontsize=12, loc="left")
        ax.set_xlabel("rows (1s candles)")
        ax.set_ylabel(ylabel)
    _dedup_legend(axes[0][0])
    fig.suptitle(title, color=INK, fontsize=14, x=0.02, ha="left", fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def speedup_chart(df, out_path):
    clean = df[~df.distorted]
    piv = clean.pivot_table(index=["pipeline", "n_rows"], columns="engine",
                            values="wall_median_s").reset_index()
    if "pandas" not in piv or "polars" not in piv:
        print("skip speedup: need both engines")
        return
    piv = piv.dropna(subset=["pandas", "polars"])
    piv["speedup"] = piv["pandas"] / piv["polars"]

    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    ax.axhline(1.0, color=MUTED, ls="--", lw=1, label="parity")
    for i, pipe in enumerate(sorted(piv.pipeline.unique())):
        s = piv[piv.pipeline == pipe].sort_values("n_rows")
        color = list(ENGINE_COLOR.values())[i % 2]
        ax.plot(s.n_rows, s.speedup, "-o", lw=2, ms=7, color=color, label=pipe)
        ax.annotate(f" {pipe}", (s.n_rows.iloc[-1], s.speedup.iloc[-1]), color=color,
                    fontsize=9, va="center")
    ax.set_xscale("log")
    ax.set_xlabel("rows (1s candles)")
    ax.set_ylabel("speedup  (pandas median / polars median)")
    ax.text(0.01, 0.98, "above 1.0 = polars faster", transform=ax.transAxes,
            color=SECOND, fontsize=9, va="top")
    _dedup_legend(ax)
    fig.suptitle("pandas vs polars speedup", color=INK, fontsize=14, x=0.02, ha="left",
                 fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="benchmarks/results/results.csv")
    ap.add_argument("--out", default="benchmarks/results/plots")
    args = ap.parse_args()

    results_csv = Path(args.results)
    if not results_csv.exists():
        raise SystemExit(f"No results at {results_csv}; run `python -m benchmarks.run_bench` first.")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    df = load(results_csv)

    small_multiples(df, "wall_median_s", "wall time (s, median) — log",
                    "Analysis pipeline compute: pandas vs polars", out / "wall_time.png")
    if "peak_mem_mb" in df:
        small_multiples(df, "peak_mem_mb", "peak RSS (MB) — log",
                        "Peak memory: pandas vs polars", out / "peak_memory.png")
    speedup_chart(df, out / "speedup.png")

    n_dist = int(df["distorted"].sum())
    if n_dist:
        print(f"note: {n_dist} distorted (mem-pressure) point(s) marked with red X, excluded from lines.")


if __name__ == "__main__":
    main()
