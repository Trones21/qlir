# Polars vs Pandas benchmark (analysis pipelines)

A runbook to measure how analysis-pipeline compute scales on **pandas** vs
**polars**, so the choice is driven by data instead of vibes.

## Why this exists / what it answers

The ETL redundancy is already solved (incremental ETL), so the open question is
the **analysis** compute: on larger, higher-frequency data, is pandas a
bottleneck, and would polars help? The honest answer is **pipeline- and
size-dependent** — this harness measures it rather than guessing.

## What's compared

Two representative pipelines, each implemented in **matched pandas + polars
twins** (so parity is easy to verify — see below). They stress different
operations because polars' relative advantage varies a lot by operation type:

- **`indicators`** — rolling mean/std + `ewm` (indicator computation).
- **`events`** — condition → segment id (`cumsum` on change) → per-segment run
  length + range via groupby (event/segment analysis).

Add more in `pipelines.py` as `<name>_pandas` / `<name>_polars`.

## Data

Synthetic **1-second** OHLCV candles. Performance depends on row count and
dtypes, not on realistic market structure, so **no production data is needed**
for a speed comparison (real data was only needed for ETL *correctness*).

1-second is the regime that matters: at 1-minute, even complex analyses finish
in seconds. Row-count → time-span reference:

| rows   | ~span of 1s candles |
|--------|---------------------|
| 100k   | ~1 day              |
| 1M     | ~11.6 days          |
| 5M     | ~2 months           |
| 30M    | ~1 year             |

**Memory is the real ceiling.** ~30M float64 rows × 6 cols ≈ 1.4 GB per copy,
and pandas ops copy — size to what the machine can hold.

## Runbook

```bash
# 0. one-time: install the bench extra (adds polars)
pip install -e '.[bench]'          # or: poetry run pip install polars

# 1. generate datasets (comma list; k/m suffixes ok)
python -m benchmarks.gen_data --rows 100k,1m,5m --out benchmarks/data

# 2. run the matrix (pipeline x engine x size), 5 repeats each
python -m benchmarks.run_bench --pipelines indicators,events \
    --engines pandas,polars --repeats 5

# 3. results:
#    benchmarks/results/results.csv   appended, one row per pipeline/engine/size
#    benchmarks/results/logs/*.json   raw per-run timings, persisted

# 4. charts from results.csv
python -m benchmarks.plot_results
#    benchmarks/results/plots/{wall_time,peak_memory,speedup}.png
```

Each config runs in its **own subprocess**, so peak memory (`ru_maxrss`) is
isolated and a big pandas run can't inflate a polars measurement.
`results.csv` and `data/` are git-ignored (environment-specific / large);
regenerate as needed.

### Verify the implementations agree (do this first / when editing pipelines)

```bash
python -m pytest tests/benchmarks/test_pipeline_parity.py
```

A faster wrong answer is not a win, so timings only count if the pandas and
polars twins produce the same output.

## Interpreting results — read this before drawing conclusions

- **Fair implementations matter more than the engine.** In a first pass, the
  naive pandas `events` used `groupby.transform(lambda x: x.max() - x.min())` —
  a Python-lambda transform — and polars looked **57–70x** faster. Switching to
  the idiomatic vectorized `transform("max") - transform("min")` collapsed that
  to pandas being *faster* than polars on `events`. Always compare
  reasonably-optimized code on both sides, or the number is meaningless.
- **Memory pressure invalidates timings — the harness guards against it.**
  Once a run spills to swap, you're timing paging, not the engine (swap is
  orders of magnitude slower than RAM). Each run records peak RSS, bytes swapped
  out during the timed section, available RAM, and the loaded frame's size. A run
  is flagged `mem_pressure` if it swapped **or** peak RSS exceeded 90% of total
  RAM; the runner prints `** MEM-PRESSURE: timing distorted **`, and the plots
  draw those points as a red X and exclude them from the fitted lines and the
  speedup. If a size is too big to load at all, the OOM killer takes only that
  isolated worker (each config runs in its own subprocess) — the run continues
  and records the config as a failure instead of crashing the whole matrix.
  Net: **push sizes up until you see mem-pressure flags or OOM failures — that
  boundary is itself a result** (it's where the engine stops fitting in RAM), and
  the timings on the clean side of it are the ones to trust.
- **Size regime.** At small sizes both engines finish in milliseconds; the
  interesting range is large data on real hardware. Push the sizes up until you
  see a divergence (or hit the memory boundary above).
- **Threads.** Polars is multi-threaded by default; its advantage grows with
  core count. Results on a small/constrained box understate polars.
- **Eager vs lazy.** These twins use eager polars for an apples-to-apples
  comparison. A lazy (`LazyFrame`) variant can optimize further — worth adding
  if a pipeline looks promising.

## Example: smoke run on a small, constrained container (16 GB, no swap)

Fair implementations, speedup = pandas median / polars median (>1 = polars faster):

| pipeline   | 50k  | 250k | 500k | 1M   |
|------------|------|------|------|------|
| indicators | 1.9x | 2.0x | 2.6x | 2.5x |
| events     | 0.71x | 0.63x | 0.50x | 0.72x |

Takeaway from this (small) sample: polars clearly wins on rolling indicators (and
the gap widens with size); pandas wins on this groupby/segment shape. **This is
why you run it on your hardware at your sizes** — the conclusion is not
universal, and it flips per pipeline.
