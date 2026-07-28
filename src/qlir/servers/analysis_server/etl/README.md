# Writing an ETL pipeline for the analysis server

The analysis server reads raw agg parquet and turns it into an analysis-ready
DataFrame every loop. **That transform is the ETL pipeline, and you write it.**
This guide covers how to plug one in, choose full vs incremental execution, and
prove it is correct — without needing production data.

## 1. What a pipeline is

A function `run_full(raw_df) -> clean_df`. It receives a raw concat of parquet
rows and returns the cleaned, analysis-ready frame (sorted, deduped, indexed,
gaps handled — whatever your data needs).

## 2. Register it and select it

```python
from qlir.servers.analysis_server.etl.pipeline_spec import ETLPipeline, register_pipeline

MY = ETLPipeline(
    name="my_v1",
    run_full=my_clean,        # your raw_df -> clean_df function
    overlap_files=1,          # see §4
    incremental_safe=True,    # see §3
    ts_col="tz_start",        # UTC, sortable, monotonic after cleaning
)
register_pipeline(MY)
```

Select it at runtime with `QLIR_ETL_PIPELINE=my_v1` (default: `candles_v1`).

## 3. Full vs incremental

`QLIR_ETL_MODE` picks the execution strategy:

- **`full_each_loop`** (default) — re-run the whole ETL every loop. Simple and
  always correct. Fine for low-frequency / small windows.
- **`incremental`** — cache the cleaned *sealed* history in memory and recompute
  only the volatile head each loop. Much faster on large data, but only correct
  if your pipeline satisfies the contract below.

The default is `full_each_loop`, so incremental is strictly opt-in.

### The incremental contract

Your pipeline must be:

1. **Deterministic.**
2. **Local** — a row's cleaned value depends only on rows within `overlap_files`
   chunks of it. Local: dedupe on timestamp, gap-fill between adjacent rows, a
   rolling window no wider than a chunk. **Not** local: a global normalization, a
   cumulative over all history, a rolling window wider than a chunk.
3. **Emits `ts_col`** (UTC, sortable, monotonic after cleaning). The provider
   splices cached history and the recomputed head at the last sealed timestamp,
   so this column must be present and ordered.

If your pipeline genuinely needs unbounded history, set `incremental_safe=False`
and the provider transparently runs it full each loop.

## 4. Choosing `overlap_files`

Set it to cover your widest *backward* dependency, measured in chunks. Dedupe and
adjacent-row gap logic need ~1 chunk of context, so `overlap_files=1` is the
common case. A feature that looks back N rows needs `ceil(N / chunk_rows)` chunks.
When unsure, start at 1 — the parity check (§5) will fail if it is too small.

## 5. Validate it — no production data required

Write a one-line test with a *representative* raw stream (synthetic is fine):

```python
from qlir.servers.analysis_server.etl.parity import assert_incremental_parity

def test_my_pipeline_is_incremental_safe(tmp_path):
    assert_incremental_parity(MY, my_representative_raw_rows(), tmp_path=tmp_path)
```

It simulates the agg server (rows arrive, chunks seal, the tail stays as the
head) and asserts `incremental == full` row-for-row at every step. If it fails,
your pipeline either violates the locality contract or `overlap_files` is too
small. (See `tests/.../etl/test_pipeline_parity_helper.py` for a worked example,
including a deliberately non-local pipeline the check correctly rejects.)

When you eventually have real data, run the same helper against a small captured
sample as the go-live gate before enabling `incremental`.

## 6. Two things to know

- **Memory.** `incremental` with `last_n_files = 0` (full dataset) holds the
  entire cleaned history in memory and grows without bound. For long-lived
  servers, use a bounded window (`last_n_files > 0`, currently `LAST_N_FILES` in
  the server config).
- **Escape hatch.** If incremental ever misbehaves in production, set
  `QLIR_ETL_MODE=full_each_loop` to return to the proven-correct path instantly.

See `../INCREMENTAL_ETL_DESIGN.md` for how the provider works internally.
