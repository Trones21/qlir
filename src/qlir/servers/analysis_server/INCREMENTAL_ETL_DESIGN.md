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

---
==============================================

## Appendix: the "fold" optimization, explained

This is a **deferred** optimization. It is written out here because the win is
narrow and the reasoning is subtle — read this before deciding to build it.

### How incremental mode works *today*

In `incremental` mode the provider holds **two** things in memory:

1. `sealed_clean` — the cleaned output of every **sealed** chunk in the window.
   Sealed chunks are immutable, so this only needs to change when the *set* of
   sealed chunks changes. It is cached and keyed by the sealed file identities
   `(name, size, mtime)`.
2. Nothing else is cached — the **head** is volatile, so it is re-read and
   re-cleaned every loop.

Each loop does:

```
recompute = clean( last_overlap_sealed_chunks  +  head )   # small, every loop
result    = sealed_clean  ++  recompute-rows-after-last-sealed-ts   # splice
```

The trailing sealed chunk is fed into `recompute` only as **left-context**, so
the first gap/dedupe right after the last sealed candle is correct. We keep
`sealed_clean` for everything up to the last sealed candle and take only the
freshly-recomputed rows after it.

The key question is: **when does `sealed_clean` get rebuilt?** Answer: whenever
the sealed set changes — and the only thing that changes it is a **rotation**
(the head fills up, gets sealed into a new numbered chunk, and a fresh head
starts). When that happens today, we **throw `sealed_clean` away and re-clean
every sealed chunk from scratch.**

### Worked example (chunks of 10 candles, `last_n_files = 0`, i.e. full dataset)

```
Loop 1   files: [part_000 (0–9)] [head (10–13)]
         sealed = {part_000}     -> sealed_clean = clean(part_000)     [BUILD]
         per loop: clean(part_000 + head), splice                       [cheap]

... head grows 10→19, sealed unchanged -> sealed_clean reused ...       [cheap]

Rotation head(10–19) becomes part_001, new head(20–…)
         sealed = {part_000, part_001} -> key changed
         -> sealed_clean = clean(part_000 + part_001)  FROM SCRATCH     [REBUILD]
```

Now imagine this has been running a long time and there are **500 sealed
chunks**. On the next rotation, "re-clean every sealed chunk from scratch" means
cleaning all 500 chunks again — just to add the one that sealed. That is the
cost. It happens **once per rotation** (rare — for 1-minute data with big
chunks, hours apart), but it is O(entire history).

### What "fold" would change

Instead of rebuilding `sealed_clean` from scratch on rotation, **extend it** by
applying the *same splice we already use for the head* — but to promote the
newly-sealed chunk into the cache:

```
# on rotation, with part_500 newly sealed and sealed_clean already covering 000–499:
TODAY:  sealed_clean = clean(part_000 + … + part_500)          # re-clean 501 chunks
FOLD:   sealed_clean = sealed_clean ++ splice( clean(part_499 + part_500) )
                                                               # clean ~2 chunks
```

`part_499` is included only as left-context for the seam, exactly like the head
splice. So a rotation would cost ~2 chunks of cleaning instead of the whole
history.

**Why this is safe:** the newly-sealed chunk is now immutable, and its boundary
with the previous chunk was *already being cleaned together every loop* while it
was the head (it lived in the `overlap + head` region). Folding just makes
permanent what we were already computing. The same `overlap_files` locality
contract that makes the head splice correct makes the fold splice correct.

### When it matters, and why it is deferred

- In **windowed mode** (`last_n_files = 5`, the current server config) the rebuild
  is *already* bounded to the last few files — "all sealed chunks in the window"
  is only 4 chunks. Fold saves essentially nothing here.
- The rebuild is only painful in **`last_n_files = 0` (full dataset) with a long
  history**, where "all sealed chunks" grows without bound.
- Fold adds a **second splice path** (promoting sealed, on top of splicing the
  head) — more seam logic, more surface area for a subtle parity bug. It would
  need its own coverage in the parity harness before being trusted.

**Recommendation:** leave it deferred. Build it only if/when full-dataset mode is
actually run at scale and the once-per-rotation rebuild is a felt problem. v1
keeps a single splice path, which the parity harness already proves correct.

## Performance logging

Follow the existing `@telemetry(console=True, log_path=...)` pattern already on
`clean_data` (`etl/pipelines/first_pipeline.py`). Instrument the new hot-path
pieces — the freshness fingerprint, the window load, and (step 2) the splice —
so each loop prints how long each stage took and idle-vs-active loops are
directly comparable.

## Testing (no real dataset required)

There are **two different things** to test, and they must not be conflated:

### 1. Does the provider machinery splice correctly? (our code)
`tests/.../io/test_clean_data_provider_parity.py` replays an evolving directory
(append, rotate, seam gap + duplicate boundary candle, window slide) and asserts
`incremental == full_each_loop` row-for-row, for a dedupe-only and a
gap-materializing pipeline, at `last_n_files` 0 and 3. This is the acceptance
gate for the splice itself.

### 2. Is a user's ETL pipeline incremental-safe? (their code)
This is the real ongoing risk: the ETL is **user-authored**, and incremental
mode only matches full mode when the pipeline's cross-row dependencies stay
within `overlap_files` chunks. A pipeline that reaches further back — a global
normalization, a cumulative over all history, a rolling window wider than the
overlap — will **silently** diverge in incremental mode.

So we ship the parity check as a reusable, framework-agnostic helper,
`etl/parity.py :: assert_incremental_parity(pipeline, raw_stream, tmp_path=...)`.
It needs no production data — only a *representative* raw stream. It simulates
the agg server (rows arrive, full chunks seal write-once, the tail stays as the
volatile head), and asserts `incremental == full` at every step and for several
window sizes. Every pipeline author should have a one-line test:

```python
def test_my_pipeline_is_incremental_safe(tmp_path):
    assert_incremental_parity(MY_PIPELINE, my_representative_raw_rows(), tmp_path=tmp_path)
```

**The checker has teeth** — proven by `tests/.../etl/test_pipeline_parity_helper.py`,
which includes a deliberately non-local pipeline (a column that depends on the
count of all rows) and asserts the helper *catches* it. A parity checker that
never fails would be worthless; this one demonstrably fails on a contract
violation.

### 3. Live parity guard on real data — DECIDED: not building it
A live guard would run **both** modes each loop for a while and alert on
divergence, turning production data into the oracle. We considered it and decided
against it, because it solves the same problem the offline helper already solves,
with far more machinery:

- The offline `assert_incremental_parity` helper runs against **any** raw stream,
  including a **small captured real sample**. So the day real data exists, the
  go-live gate is: capture one or two real parquet chunks, feed them to the same
  helper, done. No parallel instances, no shadow clean, no hot-loop coupling.
- A live guard can't be cleanly pushed off the hot loop — to validate the
  new-data splice it must see the same new-data event — so it's genuinely fiddly
  for a low-probability payoff.
- Even in the worst case (incremental silently diverges in production),
  `full_each_loop` is the default and an **instant, reversible escape hatch**:
  flip `QLIR_ETL_MODE` back to the proven-correct path.

**Go-live gate for incremental:** run `assert_incremental_parity` against a small
captured real sample before enabling `QLIR_ETL_MODE=incremental`. That is
sufficient; no continuous live validation.

The real dataset is otherwise only needed later for the (separate)
Polars-vs-pandas performance comparison.
