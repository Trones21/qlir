"""
Benchmark driver: for each (unit, engine, size) run an isolated worker, collect
wall-time + peak memory, append to results.csv, persist per-run logs, and print a
speedup summary.

A "unit" is an op (default suite `ops`) or a composed pipeline (`--suite pipelines`).

Usage:
    python -m benchmarks.run_bench                     # all ops, pandas vs polars
    python -m benchmarks.run_bench --suite pipelines
    python -m benchmarks.run_bench --units rolling_mean,groupby_transform

Results:
    benchmarks/results/results.csv     (appended, one row per unit/engine/size)
    benchmarks/results/logs/*.json     (raw per-run timings, persisted)
"""
from __future__ import annotations

import argparse
import csv
import importlib
import json
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    "timestamp",
    "suite",
    "unit",
    "unit_class",
    "engine",
    "n_rows",
    "repeats",
    "wall_median_s",
    "wall_min_s",
    "peak_mem_mb",
    "frame_mem_mb",
    "swap_out_mb",
    "swapped",
    "mem_pressure",
    "ram_avail_start_mb",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="ops", help="ops | pipelines")
    ap.add_argument("--units", default=None, help="comma list (default: all in the suite)")
    ap.add_argument("--engines", default="pandas,polars")
    ap.add_argument("--data-dir", default="benchmarks/data")
    ap.add_argument("--out", default="benchmarks/results")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--files", default=None, help="comma list of parquet files (default: all in data-dir)")
    args = ap.parse_args()

    suite = importlib.import_module(f"benchmarks.{args.suite}")
    units = args.units.split(",") if args.units else list(suite.NAMES)

    out = Path(args.out)
    (out / "logs").mkdir(parents=True, exist_ok=True)

    data_dir = Path(args.data_dir)
    files = (
        [Path(f) for f in args.files.split(",")]
        if args.files
        else sorted(data_dir.glob("candles_1s_*.parquet"), key=lambda p: p.stat().st_size)
    )
    if not files:
        sys.exit(f"No parquet files in {data_dir}; run `python -m benchmarks.gen_data` first.")

    ts = datetime.now(timezone.utc).isoformat()
    stamp = ts.replace(":", "").replace(".", "")
    rows: list[dict] = []
    failures: list[str] = []

    for f in files:
        for unit in units:
            for engine in args.engines.split(","):
                cmd = [
                    sys.executable, "-m", "benchmarks._worker",
                    "--engine", engine, "--suite", args.suite, "--unit", unit,
                    "--file", str(f), "--repeats", str(args.repeats),
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    # OOM-killed workers return -9 / 137. Subprocess isolation means
                    # only this config dies; the rest of the matrix continues.
                    reason = (
                        "OOM (worker killed)"
                        if proc.returncode in (-9, 137)
                        else f"rc={proc.returncode}"
                    )
                    tail = (proc.stderr.strip().splitlines() or [""])[-1][:180]
                    print(f"FAIL    {engine:7} {unit:18} {f.name}: {reason}  {tail}")
                    failures.append(f"{engine}/{unit}/{f.name}: {reason}")
                    continue

                m = json.loads(proc.stdout.strip().splitlines()[-1])
                (out / "logs" / f"{stamp}_{engine}_{unit}_{m['n_rows']}.json").write_text(
                    proc.stdout
                )
                row = {
                    "timestamp": ts,
                    "suite": args.suite,
                    "unit": unit,
                    "unit_class": m.get("unit_class"),
                    "engine": engine,
                    "n_rows": m["n_rows"],
                    "repeats": m["repeats"],
                    "wall_median_s": round(statistics.median(m["times"]), 6),
                    "wall_min_s": round(min(m["times"]), 6),
                    "peak_mem_mb": m["peak_mem_mb"],
                    "frame_mem_mb": m.get("frame_mem_mb"),
                    "swap_out_mb": m.get("swap_out_mb"),
                    "swapped": m.get("swapped"),
                    "mem_pressure": m.get("mem_pressure"),
                    "ram_avail_start_mb": m.get("ram_avail_start_mb"),
                }
                rows.append(row)
                flag = "  ** MEM-PRESSURE: timing distorted **" if m.get("mem_pressure") else ""
                print(
                    f"{engine:7} {unit:18} {m['n_rows']:>11,} rows  "
                    f"median {row['wall_median_s'] * 1000:9.3f} ms  peak {row['peak_mem_mb']:.0f} MB{flag}"
                )

    results_csv = out / "results.csv"
    write_header = not results_csv.exists()
    with results_csv.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(rows)
    print(f"\nAppended {len(rows)} rows to {results_csv}")

    print("\n=== speedup (pandas median / polars median; >1 = polars faster) ===")
    by: dict[tuple, dict] = {}
    cls_of: dict[str, str] = {}
    for r in rows:
        by.setdefault((r["unit"], r["n_rows"]), {})[r["engine"]] = r["wall_median_s"]
        cls_of[r["unit"]] = r.get("unit_class")
    for (unit, n), d in sorted(by.items(), key=lambda kv: (str(cls_of.get(kv[0][0])), kv[0][0], kv[0][1])):
        if d.get("pandas") and d.get("polars"):
            print(f"{cls_of.get(unit, ''):18} {unit:18} {n:>11,}  pandas/polars = {d['pandas'] / d['polars']:.2f}x")

    distorted = [r for r in rows if r.get("mem_pressure")]
    if distorted:
        print(
            f"\n!! {len(distorted)} run(s) hit memory pressure (swap / near-OOM) — "
            "timings reflect paging, not engine speed. Excluded from plots by default."
        )
    if failures:
        print(f"\n!! {len(failures)} config(s) failed (likely OOM at this size):")
        for msg in failures:
            print(f"   - {msg}")


if __name__ == "__main__":
    main()
