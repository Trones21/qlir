"""
Turn benchmarks/results/results.csv into the op-class MAP.

Charts (PNG -> benchmarks/results/plots/):
  - speedup_map.png       per-op speedup at the largest clean size, grouped by
                          op-class (the headline: where does each engine win?)
  - speedup_vs_size.png   speedup vs rows, faceted by op-class (does the winner
                          hold / widen with size?)

Runs flagged for memory pressure (swap / near-OOM) are excluded — their timings
reflect paging, not engine speed. Uses the latest run per (unit, engine, size).

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

# dataviz palette
PANDAS, POLARS = "#2a78d6", "#eb6834"           # slot-1 blue / slot-2 orange
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # for multi-line facets
INK, SECOND, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
SURFACE, AXIS = "#fcfcfb", "#c3c2b7"

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
    df = df.sort_values("timestamp").groupby(
        ["unit", "engine", "n_rows"], as_index=False
    ).last()
    df["distorted"] = df["mem_pressure"].apply(_truthy) if "mem_pressure" in df else False
    return df[~df.distorted]


def speedups(df: pd.DataFrame) -> pd.DataFrame:
    # 'conversion' is a directional cost (pandas->polars vs the reverse), not a
    # same-computation race — it doesn't belong on a speedup axis.
    df = df[df.unit_class != "conversion"]
    piv = df.pivot_table(
        index=["unit_class", "unit", "n_rows"], columns="engine", values="wall_median_s"
    ).reset_index()
    if "pandas" not in piv or "polars" not in piv:
        return pd.DataFrame()
    piv = piv.dropna(subset=["pandas", "polars"])
    piv["speedup"] = piv["pandas"] / piv["polars"]
    return piv


def map_chart(sp: pd.DataFrame, out_path: Path) -> None:
    ref_n = int(sp.n_rows.max())
    m = sp[sp.n_rows == ref_n].sort_values(["unit_class", "speedup"]).reset_index(drop=True)
    if m.empty:
        print("skip map: no data")
        return

    colors = [POLARS if s > 1 else PANDAS for s in m.speedup]
    y = range(len(m))
    fig, ax = plt.subplots(figsize=(10, 0.45 * len(m) + 1.8))
    ax.barh(list(y), m.speedup, color=colors, height=0.66, zorder=3)
    ax.axvline(1.0, color=MUTED, ls="--", lw=1, zorder=2)
    ax.set_yticks(list(y))
    ax.set_yticklabels([f"{r.unit}  ·  {r.unit_class}" for r in m.itertuples()], fontsize=9)
    ax.set_xscale("log")
    ax.set_xlabel("speedup  (pandas median / polars median)")
    for i, s in enumerate(m.speedup):
        ax.annotate(f"{s:.2f}x", (s, i), xytext=(4, 0), textcoords="offset points",
                    va="center", ha="left", fontsize=8, color=SECOND)
    # legend via proxy handles (color + label, never color alone)
    import matplotlib.patches as mpatches
    ax.legend(
        handles=[mpatches.Patch(color=POLARS, label="polars faster"),
                 mpatches.Patch(color=PANDAS, label="pandas faster")],
        frameon=False, fontsize=9, loc="lower right", labelcolor=SECOND,
    )
    ax.set_title(
        f"Op-class map: pandas vs polars  —  {ref_n:,} rows",
        color=INK, fontsize=12, loc="left", fontweight="bold",
    )
    ax.margins(y=0.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def vs_size_chart(sp: pd.DataFrame, out_path: Path) -> None:
    classes = sorted(sp.unit_class.unique())
    ncol = min(4, len(classes))
    nrow = (len(classes) + ncol - 1) // ncol
    fig, axes = plt.subplots(nrow, ncol, figsize=(4.2 * ncol, 3.4 * nrow), squeeze=False)
    for k, cls in enumerate(classes):
        ax = axes[k // ncol][k % ncol]
        sub = sp[sp.unit_class == cls]
        for i, unit in enumerate(sorted(sub.unit.unique())):
            u = sub[sub.unit == unit].sort_values("n_rows")
            c = CAT[i % len(CAT)]
            ax.plot(u.n_rows, u.speedup, "-o", color=c, lw=2, ms=6, label=unit)
            ax.annotate(f" {unit}", (u.n_rows.iloc[-1], u.speedup.iloc[-1]), color=c,
                        fontsize=8, va="center")
        ax.axhline(1.0, color=MUTED, ls="--", lw=1)
        ax.set_xscale("log")
        ax.set_title(cls, color=INK, fontsize=11, loc="left")
        ax.set_xlabel("rows")
        ax.set_ylabel("speedup")
    for k in range(len(classes), nrow * ncol):
        axes[k // ncol][k % ncol].axis("off")
    fig.suptitle("Speedup vs size, by op-class  (>1 = polars faster)", color=INK,
                 fontsize=13, x=0.02, ha="left", fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.96))
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
    sp = speedups(load(results_csv))
    if sp.empty:
        raise SystemExit("Need both pandas and polars results to plot speedup.")

    map_chart(sp, out / "speedup_map.png")
    vs_size_chart(sp, out / "speedup_vs_size.png")


if __name__ == "__main__":
    main()
