"""
Benchmark driver: for each (pipeline, engine, size) run an isolated worker,
collect wall-time + peak memory, append to results.csv, persist per-run logs,
and print a speedup summary.

Usage:
    python -m benchmarks.run_bench --pipelines indicators,events \
        --engines pandas,polars --repeats 5

Results:
    benchmarks/results/results.csv     (appended, one row per pipeline/engine/size)
    benchmarks/results/logs/*.json     (raw per-run timings, persisted)
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    "timestamp",
    "pipeline",
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
    ap.add_argument("--pipelines", default="indicators,events")
    ap.add_argument("--engines", default="pandas,polars")
    ap.add_argument("--data-dir", default="benchmarks/data")
    ap.add_argument("--out", default="benchmarks/results")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--files", default=None, help="comma list of parquet files (default: all in data-dir)")
    args = ap.parse_args()

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
        for pipeline in args.pipelines.split(","):
            for engine in args.engines.split(","):
                cmd = [
                    sys.executable, "-m", "benchmarks._worker",
                    "--engine", engine, "--pipeline", pipeline,
                    "--file", str(f), "--repeats", str(args.repeats),
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    # A worker killed by the OOM killer returns -9 / 137. Isolation
                    # means only this config dies; the rest of the matrix continues.
                    reason = (
                        "OOM (worker killed)"
                        if proc.returncode in (-9, 137)
                        else f"rc={proc.returncode}"
                    )
                    tail = (proc.stderr.strip().splitlines() or [""])[-1][:180]
                    print(f"FAIL    {engine:7} {pipeline:11} {f.name}: {reason}  {tail}")
                    failures.append(f"{engine}/{pipeline}/{f.name}: {reason}")
                    continue

                m = json.loads(proc.stdout.strip().splitlines()[-1])
                (out / "logs" / f"{stamp}_{engine}_{pipeline}_{m['n_rows']}.json").write_text(
                    proc.stdout
                )
                row = {
                    "timestamp": ts,
                    "pipeline": pipeline,
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
                    f"{engine:7} {pipeline:11} {m['n_rows']:>11,} rows  "
                    f"median {row['wall_median_s'] * 1000:9.2f} ms  peak {row['peak_mem_mb']:.0f} MB{flag}"
                )

    results_csv = out / "results.csv"
    write_header = not results_csv.exists()
    with results_csv.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(rows)
    print(f"\nAppended {len(rows)} rows to {results_csv}")

    print("\n=== speedup (pandas median / polars median) ===")
    by: dict[tuple, dict] = {}
    for r in rows:
        by.setdefault((r["pipeline"], r["n_rows"]), {})[r["engine"]] = r["wall_median_s"]
    for (pipe, n), d in sorted(by.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        if d.get("pandas") and d.get("polars"):
            print(f"{pipe:11} {n:>11,} rows  pandas/polars = {d['pandas'] / d['polars']:.2f}x")

    distorted = [r for r in rows if r.get("mem_pressure")]
    if distorted:
        print(
            f"\n!! {len(distorted)} run(s) hit memory pressure (swap / near-OOM) — "
            "their timings reflect paging, not engine speed. Excluded from plots by default."
        )
    if failures:
        print(f"\n!! {len(failures)} config(s) failed (likely OOM at this size):")
        for msg in failures:
            print(f"   - {msg}")


if __name__ == "__main__":
    main()
