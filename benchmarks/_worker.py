"""
Run one (engine, pipeline) over a parquet file N times and print JSON metrics.

Runs in its own process (launched by run_bench.py) so peak memory
(resource.ru_maxrss) is isolated per configuration and a big pandas run cannot
inflate a polars measurement.
"""
from __future__ import annotations

import argparse
import json
import resource
import time

import pandas as pd
import psutil

from benchmarks import pipelines


def load(engine: str, path: str):
    if engine == "pandas":
        return pd.read_parquet(path)
    import polars as pl

    return pl.read_parquet(path)


def frame_mem_bytes(frame) -> int:
    if isinstance(frame, pd.DataFrame):
        return int(frame.memory_usage(deep=True).sum())
    return int(frame.estimated_size())  # polars


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True)
    ap.add_argument("--pipeline", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()

    vm = psutil.virtual_memory()
    frame = load(args.engine, args.file)
    fn = pipelines.get(args.pipeline, args.engine)

    fn(frame)  # warmup (imports / lazy caches) — not timed

    sw0 = psutil.swap_memory()
    times = []
    for _ in range(args.repeats):
        t0 = time.perf_counter()
        result = fn(frame)
        _ = len(result)  # force materialization
        times.append(time.perf_counter() - t0)
    sw1 = psutil.swap_memory()

    # Linux: ru_maxrss is in KB.
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    swap_out_mb = max(0, sw1.sout - sw0.sout) / 1e6  # bytes paged OUT during the timed runs
    # "Distorted" = the run touched swap, OR peak RSS ate most of RAM (near-OOM,
    # where the OS is under memory pressure even without configured swap).
    swapped = swap_out_mb > 5.0
    mem_pressure = swapped or (peak_mb > 0.9 * (vm.total / 1e6))

    print(
        json.dumps(
            {
                "engine": args.engine,
                "pipeline": args.pipeline,
                "n_rows": int(len(frame)),
                "repeats": args.repeats,
                "times": times,
                "peak_mem_mb": round(peak_mb, 1),
                "frame_mem_mb": round(frame_mem_bytes(frame) / 1e6, 1),
                "ram_total_mb": round(vm.total / 1e6, 1),
                "ram_avail_start_mb": round(vm.available / 1e6, 1),
                "swap_out_mb": round(swap_out_mb, 1),
                "swapped": swapped,
                "mem_pressure": mem_pressure,
            }
        )
    )


if __name__ == "__main__":
    main()
