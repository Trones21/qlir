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
- **Size regime.** At small sizes both engines finish in milliseconds; the
  interesting range is large data on real hardware. Push the sizes up until you
  see a divergence (or hit memory).
- **Threads.** Polars is multi-threaded by default; its advantage grows with
  core count. Results on a small/constrained box understate polars.
- **Eager vs lazy.** These twins use eager polars for an apples-to-apples
  comparison. A lazy (`LazyFrame`) variant can optimize further — worth adding
  if a pipeline looks promising.

## Example: smoke run on a small, constrained container

Fair implementations, 3 repeats, median wall time:

| pipeline   | rows | pandas | polars | pandas/polars |
|------------|------|--------|--------|---------------|
| indicators | 50k  | 3.8 ms | 2.2 ms | 1.72x |
| indicators | 500k | 29 ms  | 16 ms  | 1.82x |
| events     | 50k  | 7.3 ms | 11 ms  | 0.68x (pandas faster) |
| events     | 500k | 55 ms  | 88 ms  | 0.63x (pandas faster) |

Takeaway from this (small) sample: polars modestly wins on rolling indicators;
pandas wins on this groupby shape at these sizes. **This is why you run it on
your hardware at your sizes** — the conclusion is not universal.
