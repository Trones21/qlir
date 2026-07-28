# agg_server — aggregation service

The aggregation server is the **second stage** of the QLIR pipeline
(`data_server → agg_server → analysis_server → notification_server`; see
[../README.md](../README.md)). It compacts the raw JSON slices written by the
`data_server` into scan-efficient, columnar **Parquet** parts.

> **KMM (Key Mental Model)**
> agg_server is a *packer*, not a transformer. It bundles immutable raw slices into
> immutable Parquet files and indexes them in a manifest. It does **no** validation,
> cleaning, resampling, gap-filling, or interpretation — those belong to later layers.

This directory is just the **service entrypoint**. All the real logic lives in
[`qlir.data.agg`](../../data/agg/), which has its own in-depth design doc:
**[../../data/agg/README.md](../../data/agg/README.md)** — read that for the full
aggregation contract, selection strategy, failure semantics, and rebuild story.
This file documents how to *run and operate the process*.

---

## What it does

Per loop, the daemon ([`qlir.data.agg.engine.run_agg_daemon`](../../data/agg/engine.py)):

1. **Waits for / loads the raw manifest** (`raw/.../manifest.json`) written by the data server.
2. **Refreshes `head` first** — the current (most recent) slice keeps growing as new
   candles land (status `partial`), so its Parquet is re-materialized from raw JSON every
   loop, *before* anything else. Slices not in `head` are treated as immutable.
3. **Selects un-aggregated slices** — eligible raw slices (`slice_status ∈ {complete, partial}`)
   that are not already in a sealed part or in `head`, sorted **oldest-first** by `start_ms`.
4. **Packs slices into `head`, sealing parts** — once `head` holds `≥ batch_slices` slices,
   it seals a `part-NNNNNN.parquet` (rows sorted by `open_time`, slice boundaries preserved),
   and the remainder stays in `head`.
5. **Records per-slice failures** in the agg manifest (never in raw) and skips them — a bad
   slice never blocks the others.

It is **availability-driven, not time-window-driven**: it reacts to new raw slices appearing,
applying natural backpressure. The agg dataset is fully **rebuildable** — it derives all state
from the raw manifest and never mutates raw, so you can delete the `agg/` tree and regenerate it.

---

## Running it

```bash
poetry run agg_server \
  --endpoint klines \
  --symbol SOLUSDT \
  --interval 1m \
  --limit 1000 \
  --batch-slices 1000
```

One process is isolated to a single `(endpoint, symbol, interval, limit)` tuple — exactly
like the data server. See [../tmux/aggsol1m.sh](../tmux/aggsol1m.sh),
[../shorthand_starters/](../shorthand_starters/) (e.g. `aggsol1s.sh`, `agg-reset.sh`), and
[../start_all_simple.sh](../start_all_simple.sh).

### CLI arguments

| Flag | Required | Meaning |
|---|---|---|
| `--datasource` | no | `binance` (default) or `interactive_brokers`. Selects the top-level data dir **and** the raw-response parser (Binance klines vs IBKR bars). |
| `--endpoint` | yes | `klines`/`uiklines` (binance) or `historical_bars` (ibkr). Selects the data directory. |
| `--symbol` | yes | Single symbol, e.g. `SOLUSDT`, `BTCUSDT`, `AAPL`. |
| `--interval` | yes | Bar interval, e.g. `1s`, `1m`. |
| `--limit` | yes | Raw slice size; used to locate the matching raw directory (`limit=<n>`). |
| `--batch-slices` | yes | Number of slices packed into each sealed Parquet part. |

To aggregate IBKR data instead of Binance:

```bash
poetry run agg_server --datasource interactive_brokers --endpoint historical_bars \
  --symbol AAPL --interval 1m --limit 1000 --batch-slices 1000
```

The datasource picks the slice loader ([schema_binance_klines.py](../../data/agg/schema_binance_klines.py)
vs [schema_ibkr_bars.py](../../data/agg/schema_ibkr_bars.py)); everything else (head/part
sealing, manifest) is identical.

`--batch-slices` sets **both** `AggConfig.batch_slices` (slices per part) and
`AggConfig.ingest_chunk_slices` (slices loaded per poll). It is a **layout/throughput knob,
not a correctness knob** — changing it changes how rows are grouped into files, not the data.

### Other tunables (not exposed on the CLI)

Defaults in [`AggConfig`](../../data/agg/engine.py); edit
[run_server.py](run_server.py) to change them:

| Field | Default | Meaning |
|---|---|---|
| `sleep_idle_s` | `20` | Sleep when there is no new work. |
| `sleep_partial_s` | `7` | Sleep when only a partial batch is available. |
| `log_every_loop` | `True` | Print a per-loop `todo/used/parts/head` summary. |

> Note: logging is infra-owned and currently hardcoded to `LogProfile.QLIR_DEBUG` in
> [run_server.py](run_server.py) (verbose). Lower it there if you want quieter logs.

---

## Inputs and outputs (on disk)

Resolved under `QLIR_DATA_ROOT` (default `~/qlir_data`). Paths are computed in
[run_server.py](run_server.py) and modeled by
[`DatasetPaths`](../../data/agg/paths.py):

```
$QLIR_DATA_ROOT/binance/<endpoint>/
  raw/<symbol>/<interval>/limit=<limit>/        # INPUT  (owned by data_server)
    manifest.json                               #   slice ledger it polls
    responses/<slice_id>.json                   #   raw kline arrays it reads
  agg/<symbol>/<interval>/limit=<limit>/        # OUTPUT (owned by agg_server)
    manifest.json                               #   parts + head + slice_failures index
    parts/part-NNNNNN.parquet                   #   sealed, immutable chunks
    parts/head.parquet                          #   rolling, re-written buffer (current slice)
```

Each Parquet part carries the standard 12-column Binance kline schema (see
[schema_binance_klines.py](../../data/agg/schema_binance_klines.py)):
`open_time, open, high, low, close, volume, close_time, quote_asset_volume, num_trades,
taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore`. Coercions are mechanical
only (ints/floats) — no semantic cleanup.

Downstream, the analysis server reads these parts directly (a rolling window of the most
recent ones + `head.parquet`).

---

## Agg manifest shape (as written by the code)

The authoritative shape comes from [manifest.py](../../data/agg/manifest.py). Note this uses
**`slice_ids`** (the engine doc's older examples say `slice_hashes` — the code is the source
of truth), and also tracks `head` and `slice_failures`:

```jsonc
{
  "dataset": { "source": "binance", "dataset": "klines",
               "symbol": "SOLUSDT", "interval": "1m", "limit": 1000 },
  "created_at": "2026-01-01T00:00:00+00:00",
  "updated_at": "2026-01-01T00:05:00+00:00",
  "parts": [
    {
      "part": "parts/part-000001.parquet",
      "slice_ids": ["e901567f...", "a14c33be..."],
      "row_count": 1000000,
      "min_open_time": 1610462400000,   // hints for query engines, NOT a contiguity claim
      "max_open_time": 1610894340000,
      "created_at": "2026-01-01T00:01:00+00:00"
    }
  ],
  "head": {                              // remainder < batch_slices; re-written each loop
    "items": [ { "slice_id": "9fa0021c...", "row_count": 432 } ],
    "row_count": 432,
    "min_open_time": 1610894400000,
    "max_open_time": 1610894740000
  },
  "slice_failures": {                    // load/parse failures, recorded here (never in raw)
    "deadbeef...": { "error": "ValueError: ...", "failed_at": "2026-01-01T00:02:00+00:00" }
  }
}
```

`slice_ids` are the unit of identity/provenance; the part filename is just a label, and
`min/max_open_time` are query hints, not range-completeness guarantees.

---

## Crash safety & rebuilds

- **Atomic writes:** Parquet and manifest writes use `.tmp` + atomic rename
  ([atomic.py](../../data/agg/atomic.py)), so readers never see a half-written file and a
  crash mid-write leaves the manifest consistent.
- **At-most-once:** each raw slice is aggregated at most once (tracked by `slice_ids` across
  `parts` + `head`).
- **Rebuild:** because agg never writes back to raw, you can delete the `agg/<symbol>/...`
  tree and the daemon will regenerate it from the raw manifest (a late rebuild also improves
  temporal locality within parts). See `agg-reset.sh` in
  [../shorthand_starters/](../shorthand_starters/).

---

## See also

- **[../../data/agg/README.md](../../data/agg/README.md)** — full aggregation-layer design doc
  (contract, selection strategy, failure handling, mental model).
- [../README.md](../README.md) — top-level pipeline infrastructure overview.
- [../data_server/README.md](../data_server/README.md) — the upstream ingest stage and raw manifest model.
