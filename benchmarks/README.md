# Polars vs Pandas — op-class map (technical-analysis library)

A runbook to measure **which operation classes** are faster in pandas vs polars,
so the choice is driven by a durable map instead of per-pipeline surprises.

## Why the unit is the *operation*, not the pipeline

A pipeline is a composition of primitives, and the engine winner is a property of
the **op-class** (rolling, groupby-transform, cumulative, …), not the pipeline.
Benchmarking whole pipelines gave a different answer each time — because each
pipeline is a different *mix* of op-classes. Benchmarking the primitives instead
yields a **map** ("rolling → ~1.2x polars, groupby → polars at scale, segment →
pandas, …") that generalizes to any pipeline built from them.

This is a **library** concern, not the analysis server: the engine decision is
made per primitive (`indicators.sma`, the groupby bundles, …).

## Two suites

- **`ops`** (default) — primitive micro-benchmarks in `ops.py`, tagged by
  op-class. This produces the map.
- **`pipelines`** — composed pipelines in `pipelines.py`. Kept as a *validation*
  layer: does a pipeline's measured time ≈ the sum of its ops? If yes, the map is
  trustworthy for reasoning about pipelines.

Each op/pipeline is implemented as matched pandas + polars twins; parity is
enforced (`tests/benchmarks/test_op_parity.py`, `test_pipeline_parity.py`) so a
faster wrong answer never counts.

## Data

Synthetic **1-second** OHLCV candles. Speed depends on row count and dtypes, not
market realism, so **no production data is needed**. 1s is the regime that
matters (1-minute analyses finish in seconds). Row-count → span: 100k ≈ 1 day,
1M ≈ 11.6 days, 5M ≈ 2 months, 30M ≈ 1 year. **Memory is the ceiling** (~30M
float64 × 6 cols ≈ 1.4 GB per copy; pandas ops copy).

## Runbook

```bash
pip install -e '.[bench]'                                   # adds polars

python -m benchmarks.gen_data  --rows 100k,1m,5m            # sized 1s candles
python -m pytest tests/benchmarks/                          # verify twins agree
python -m benchmarks.run_bench --suite ops --repeats 5      # the op map
python -m benchmarks.plot_results                           # -> results/plots/*.png

# validation layer (optional): do the composed pipelines match the sum of ops?
python -m benchmarks.run_bench --suite pipelines --repeats 5
```

Outputs: `results/results.csv` (appended, one row per unit/engine/size),
`results/logs/*.json` (raw), `results/plots/{speedup_map,speedup_vs_size}.png`.
`data/` and `results/` are git-ignored (regenerable / environment-specific).

## Reading the results — before you conclude anything

- **Fair implementations matter more than the engine.** A naive pandas
  `groupby.transform(lambda …)` once made polars look 57–70x faster; the
  idiomatic vectorized form flipped it. `ops.py` uses idiomatic code on both
  sides — keep it that way when you add ops.
- **Memory pressure invalidates timings — the harness guards it.** Each run
  records peak RSS, bytes swapped out during timing, available RAM, and frame
  size. A run is flagged `mem_pressure` if it swapped **or** peak RSS exceeded
  90% of RAM; the runner prints `** MEM-PRESSURE **` and the plots drop those
  points. If a size can't load at all, the OOM killer takes only that isolated
  worker (each config is a subprocess) and it's recorded as a failure — the run
  continues. **Push sizes up until you hit mem-pressure / OOM; that boundary is
  itself a result**, and trust only the clean side of it.
- **Conversion cost is measured, not on the speedup axis.** The `convert` op
  times pandas→polars and polars→pandas — the tax you'd pay if you *mixed*
  engines per op inside one pipeline. It's excluded from the speedup map (it's a
  directional cost, not a same-computation race), but it lives in `results.csv`.
  It matters the moment you consider a mixed library: per-op wins can be erased
  by converting between them.
- **Threads / hardware.** Polars is multi-threaded; its advantage grows with core
  count. A small/constrained box understates it. Run on your hardware.

## Example: op map on a small container (16 GB, 1M rows, 3 repeats)

Speedup = pandas / polars (>1 = polars faster). Illustrative — noisy at small
sizes / few repeats:

| op (class) | speedup | | op (class) | speedup |
|---|---|---|---|---|
| unique_rows (dedup) | 3.7x | | rolling_std (rolling) | 1.07x |
| groupby_agg (reduction) | 2.1x | | diff (elementwise) | 0.96x |
| sort (reshape) | 1.8x | | cummax (cumulative) | 0.84x |
| groupby_transform | 1.6x | | segment_id (segment) | 0.64x |
| ewm_mean / rolling_mean | ~1.2x | | filter_mask (filter) | 0.30x |

Emerging shape: **polars wins on group/sort/dedup (and widens with size); pandas
holds on segment/filter/elementwise/cumulative.** So a rolling+groupby-heavy
pipeline leans polars; a segment/filter-heavy one leans pandas. Run it at your
sizes on your hardware to firm up the map — then read any pipeline off it.
