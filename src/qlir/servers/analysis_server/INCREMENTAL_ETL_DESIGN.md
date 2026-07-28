# Incremental ETL — Design & Plan

Status: **agreed, implementation in progress on branch
`claude/analysis-server-etl-full-vs-incremental-kh3rj5`.**
Do not merge until validated against a real dataset.

---

## Problem

The analysis-server main loop (`server.py`) currently runs the **full ETL every
loop iteration**:

```
while True:
    base_df = get_clean_data()   # read parquet window + run clean pipeline
    ... staleness alert ...
    if data_ts <= last_processed_ts: sleep; continue   # watermark gate
    ... materialize / events / triggers ...
```

The ETL runs **before** the watermark gate, so every poll (`POLL_INTERVAL_SEC`)
pays a full read + clean even when no new data has arrived. On the first pass
that work is unavoidable; on every subsequent pass it is almost entirely
redundant.

## Key facts that shape the design

- **The analysis server is read-only.** It never writes parquet. All cleaning /
  gap-materialization is in-memory, derived per-loop from what it reads.
- **The agg server (upstream) produces the chunks** and does *not* backfill gaps
  into them. So at chunk granularity the input is **append-only**: sealed chunks
  are immutable, and **`head.parquet` is the only volatile file**.
- The ETL is a **whole-frame, boundary-stateful** transform (dedupe groups by
  timestamp; gap logic compares adjacent rows). You cannot ETL new rows in
  isolation and append — the seam between old and new data must be reprocessed.
- **The ETL and the analysis flow are user-authored.** Our job is to give them a
  contract + guidance so their pipeline works correctly in both full and
  incremental modes — not to hardcode one pipeline's internals.

## Model: sealed cache + head recompute

```
[ sealed chunk 0 ][ sealed chunk 1 ] ... [ sealed chunk N ][ head.parquet ]
 \___________________ cached in memory ___________________/ \__ volatile __/
                                          \_ overlap _/\___ recompute region ___/
```

- **Cache** = ETL output of all *sealed* chunks already folded in, held in memory
  across loops.
- **Recompute region each loop** = `{any newly-sealed chunks} + head`, plus a
  **one-file overlap** back into the cache so dedupe / gap logic across the
  head↔prior-chunk seam is correct.
- **Provenance is file-level, not per-row:** a small `sealed_manifest`
  (per sealed file → `last_ts`, `mtime`, `n_rows`) + a `seal_watermark`
  timestamp. This is enough to (a) know the cache/recompute boundary and
  (b) detect **head rotation** — when the old head becomes a numbered chunk it
  shows up as "newly sealed", gets folded into the cache, and a fresh empty
  `head.parquet` starts. (Per-row source tags break down the moment a pipeline
  *synthesizes* gap rows that belong to no file, so we avoid them.)

Each loop, incremental mode:
1. Read raw for `last_sealed_chunk (overlap) + newly_sealed + head`.
2. Run the **full user ETL pipeline** on just that contiguous slice.
3. Splice: drop the overlap tail from the cache, append the freshly-ETL'd region.
4. Promote newly-sealed files into `sealed_manifest`; the head portion stays
   volatile and is recomputed again next loop (unless it has rotated).

## Exposing it (config surface)

Mirror the existing registry idiom (`DF_REGISTRY` for derived DFs) and the
existing env-driven config (`QLIR_ANALYSIS_*`):

- ETL pipeline becomes a small spec instead of a bare imported function:
  `ETLPipeline(name, run_full, overlap_files=1, incremental_safe=True)`.
  - `incremental_safe=False` → engine transparently falls back to
    full-each-loop (for a pipeline that genuinely needs unbounded history).
- Mode via env: `QLIR_ETL_MODE = full_each_loop | incremental`.
  - Default `full_each_loop` — identical to today's behavior **and the parity
    oracle** for validating incremental.
- `load_clean_data` becomes a stateful `CleanDataProvider` that owns the cache +
  splice; `main()` instantiates it once and calls `.get()` per loop (today it
  calls the stateless `get_clean_data()`).

## Rollout (two commits, same branch)

**Step 1 — Freshness gate (safe, standalone, ships first).**
Before the ETL, compute a cheap directory fingerprint
`(file_count, max_mtime_ns)`. If it is unchanged since the previous loop, no new
data has landed, so:
- still evaluate the **staleness alert** against the last known data ts
  (`last_processed_ts`) — staleness must keep firing while data goes stale, and
- skip the ETL + analysis and sleep.

The fingerprint over-approximates "new data" (any write bumps it), so it never
wrongly skips real new data — the safe direction. This alone removes the wasted
ETL on idle loops and needs no in-memory cache.

**Step 2 — Incremental provider. [IMPLEMENTED]**
- `etl/pipeline_spec.py` — `ETLPipeline(name, run_full, overlap_files,
  incremental_safe, ts_col)` + a tiny registry (`candles_v1` = the existing
  `clean_data`). Selected via `QLIR_ETL_PIPELINE`.
- `io/parquet_dir.py` — classify a dir into `(sealed_sorted, head)`, read a file
  list, and key a sealed set by `(name, size, mtime_ns)`.
- `io/clean_data_provider.py` — `CleanDataProvider` with `full_each_loop` and
  `incremental` modes. Splice = keep cached `sealed_clean`, take only recomputed
  head rows strictly after the last sealed ts (`cut`); trailing sealed chunks are
  recomputed each loop purely as left-context. `incremental_safe=False` falls
  back to full-each-loop.
- `server.py` — builds one provider in `main()`, calls `provider.get()` per loop.
  Mode via `QLIR_ETL_MODE` (default `full_each_loop`, so default behavior is
  unchanged).

Parity is proven by `tests/.../io/test_clean_data_provider_parity.py`: an
evolving-directory replay (append, rotate, seam gap + duplicate boundary candle,
window slide) asserts `incremental == full_each_loop` row-for-row, for both a
dedupe-only pipeline and a gap-*materializing* one, at `last_n_files` 0 and 3.

### Known cost / future optimization
`sealed_clean` is recomputed in full whenever the sealed set changes (a rotation,
or a window slide when `last_n_files > 0`). Between those events — i.e. every
idle/append loop — only `overlap_files` sealed chunks + head are recomputed, which
is the common case and the whole win. For `last_n_files = 0` (full dataset) a
rotation therefore re-cleans the entire sealed history once. A later optimization
is to *fold* a newly-sealed chunk into `sealed_clean` incrementally (same overlap
splice, one seam) instead of recomputing all sealed — deferred to keep v1 simple
and obviously correct.

## Performance logging

Follow the existing `@telemetry(console=True, log_path=...)` pattern already on
`clean_data` (`etl/pipelines/first_pipeline.py`). Instrument the new hot-path
pieces — the freshness fingerprint, the window load, and (step 2) the splice —
so each loop prints how long each stage took and idle-vs-active loops are
directly comparable.

## Validation without a real dataset

We do **not** need the production dataset to prove correctness. Build **synthetic
candle fixtures** with dupes and gaps placed exactly on chunk boundaries, then
assert `full_each_loop` output == `incremental` output **row-for-row** over a
replayed sequence of chunk/head states. That parity harness is the acceptance
gate; the real dataset is only needed later for the (separate) Polars-vs-pandas
performance comparison.
