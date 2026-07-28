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

from benchmarks import pipelines


def load(engine: str, path: str):
    if engine == "pandas":
        return pd.read_parquet(path)
    import polars as pl

    return pl.read_parquet(path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True)
    ap.add_argument("--pipeline", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()

    frame = load(args.engine, args.file)
    fn = pipelines.get(args.pipeline, args.engine)

    fn(frame)  # warmup (imports / lazy caches) — not timed

    times = []
    for _ in range(args.repeats):
        t0 = time.perf_counter()
        result = fn(frame)
        _ = len(result)  # force materialization
        times.append(time.perf_counter() - t0)

    # Linux: ru_maxrss is in KB.
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

    print(
        json.dumps(
            {
                "engine": args.engine,
                "pipeline": args.pipeline,
                "n_rows": int(len(frame)),
                "repeats": args.repeats,
                "times": times,
                "peak_mem_mb": round(peak_mb, 1),
            }
        )
    )


if __name__ == "__main__":
    main()
