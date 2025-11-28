# QLIR Binance Data Module

This module manages **raw data ingestion from Binance**, with a focus on:

- Deterministic slicing  
- Completeness tracking  
- Robust re-fetching with backoff  
- Clean, inspectable on-disk formats  
- Easy future migration to SQLite or other storage backends  

The design prioritizes **local reasoning**, **debuggability**, and **extensibility**.  
You can add new endpoints later without touching existing ones.

---

## Quickstart

```python
from qlir.data.sources.binance.server import start_data_server

start_data_server()
```

This launches the Binance data subsystem, starting one or more **endpoint workers**
(currently: a worker for `/api/v3/klines`).

Each worker:

1. Determines the time range you want (start_ms → current time).
2. Slices it into deterministic chunks (“slices”).
3. Compares expected slices to on-disk manifests.
4. Fetches missing or failed slices.
5. Persists raw responses via `qlir.io.writer`.
6. Marks slice status appropriately.

The loop continues indefinitely, keeping the raw dataset complete.

---

## Module Structure

All Binance-specific ingestion code lives under:

```text
qlir/
  data/
    sources/
      binance/
        README.md
        __init__.py

        server.py          # Entry point: start_data_server()

        completeness/
          __init__.py
          registry.py      # Compute expected slices, diff vs manifest

        endpoints/
          __init__.py

          klines/
            __init__.py
            model.py       # KlineSliceKey, SliceStatus, composite_key()
            urls.py        # interval_to_ms(), generate_kline_slices(), build_kline_url()
            fetch.py       # Call Binance and write raw responses to disk
            worker.py      # Main event loop for kline completeness
```

### Key pieces

* **`server.py`**

  * Provides `start_data_server()`.
  * Orchestrates which endpoint workers run (klines, later fundingRate, aggTrades, etc).

* **`endpoints/klines/model.py`**

  * Defines the slice model for klines:
    `KlineSliceKey(symbol, interval, start_ms, end_ms, limit=1000)`
  * Exposes `composite_key()` → canonical identifier for a slice.
  * Defines `SliceStatus` enum: `"pending" | "ok" | "failed"`.

* **`endpoints/klines/urls.py`**

  * Converts `interval` (`"1s"`, `"1m"`) → milliseconds.
  * Slices `[start_ms, end_ms]` into `(slice_start, slice_end)` windows of at most `limit=1000` candles.
  * Builds full `/api/v3/klines` URLs for each slice.

* **`endpoints/klines/fetch.py`**

  * Given a `KlineSliceKey`, builds the URL and calls Binance.
  * Wraps the raw JSON with metadata (`meta.url`, `meta.slice`, `meta.fetched_at`).
  * Delegates persistence to `qlir.io.writer` (e.g. `write_raw_response(slice_id, payload)`).

* **`endpoints/klines/worker.py`**

  * The main completeness loop for klines:

    * Gets `(min_start_ms, max_end_ms)` from some range provider.
    * Uses `urls.generate_kline_slices()` to build the expected slice universe.
    * Loads `manifest.json` (or other state) and computes missing/failed slices via `completeness.registry`.
    * Fetches each missing slice with exponential backoff on errors.
    * Updates slice status in the manifest.

* **`completeness/registry.py`**

  * Glue between URL/slice generation and on-disk status:

    * `compute_expected_slices(...)` → yields `KlineSliceKey`s.
    * `compute_missing_slices(expected, known_statuses)` → filters to slices that are still pending/failed.

This structure is repeated per endpoint (e.g. `funding/`, `aggTrades/`) so each one remains self-contained.

---

## Data Root

All raw datasets are stored under a *configurable* root directory.

Logic (simplified):

```python
def get_data_root(user_root=None):
    # 1. explicit user_root
    # 2. QLIR_DATA_ROOT env var
    # 3. ~/qlir_data (default)
```

Default:

```text
~/qlir_data/
```

---

## On-Disk Layout

The full path for Binance raw data:

```text
<data_root>/binance/<endpoint>/raw/<symbol>/<interval>/
```

Example:

```text
~/qlir_data/
  binance/
    klines/
      raw/
        BTCUSDT/
          1m/
            manifest.json
            responses/
              3af2b1e7c442b7be.json
              821cc2bd18cc4f12.json
          1s/
            manifest.json
            responses/
              ...
        ETHUSDT/
          1m/
            manifest.json
            responses/
              ...
```

Why this structure?

* **Local reasoning** – `BTCUSDT/1m` is everything for that pair & interval.
* **Easy cleanup** – delete one folder to reset/rebuild.
* **Good bucketing** – can be extended with `year=2024/` etc. if needed later.
* **Portable** – just directories + JSON files.

---

## Manifest Files

Each symbol/interval has a single:

```text
manifest.json
```

This is the “source of truth” for:

* What slice windows *should* exist
* What slices *already* exist
* Their statuses (`pending`, `ok`, `failed`)
* Their local filenames
* Optional summary stats

### Example manifest.json

```jsonc
{
  "version": 1,
  "endpoint": "klines",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "limit": 1000,

  "summary": {
    "total_slices": 1234,
    "ok_slices": 1200,
    "failed_slices": 34,
    "last_evaluated_at": "2025-11-27T19:30:00Z"
  },

  "slices": {
    "BTCUSDT:1m:1609459200000-1609465199999:1000": {
      "slice_id": "3af2b1e7c442b7be",
      "relative_path": "responses/3af2b1e7c442b7be.json",
      "status": "ok",
      "http_status": 200,
      "n_items": 1000,
      "first_ts": 1609459200000,
      "last_ts": 1609462799000,
      "requested_at": "2025-01-01T00:01:23Z",
      "completed_at": "2025-01-01T00:01:24Z",
      "error": null
    },

    "BTCUSDT:1m:1609465200000-1609471199999:1000": {
      "slice_id": "821cc2bd18cc4f12",
      "relative_path": "responses/821cc2bd18cc4f12.json",
      "status": "failed",
      "http_status": 500,
      "n_items": 0,
      "first_ts": null,
      "last_ts": null,
      "requested_at": "2025-01-01T00:03:10Z",
      "completed_at": "2025-01-01T00:03:11Z",
      "error": "HTTP 500 from Binance"
    }
  }
}
```

Notes:

* `slices` is a **dict**, keyed by `composite_key` → O(1) lookup.
* `slice_id` is a **hash** of the composite key (e.g. BLAKE2b-128), used as filename.
* `relative_path` points into the `responses/` folder.
* This schema maps cleanly to SQL columns later if you move to SQLite.

---

## Slice Keys

Each requestable window of data is described by:

* `symbol`
* `interval` (`"1s"` or `"1m"` for now)
* `start_ms`
* `end_ms`
* `limit` (always `1000` in this module)

Example composite key:

```text
BTCUSDT:1m:1609459200000-1609465199999:1000
```

Properties:

* 1:1 with a Binance URL.
* 1:1 with a single raw response file.
* Never overlaps with other slices.
* Deterministically partitions the dataset.

---

## Raw Response Files

Inside each `responses/` folder, the raw Binance payload is stored as:

```text
responses/<slice_id>.json
```

Example:

```jsonc
{
  "meta": {
    "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...",
    "slice": "BTCUSDT:1m:1609459200000-1609465199999:1000",
    "fetched_at": "2025-01-01T00:01:23Z"
  },

  "data": [
    [1609459200000, "29384.1", "29390.4", "29380.1", "29385.2", "12.123", ...],
    ...
  ]
}
```

No transformation is performed at this layer.
All interpretation (e.g. converting to DataFrames, resampling, normalization) lives in higher-level QLIR code.

---

## URL Generation

Located in:

```text
qlir/data/sources/binance/endpoints/klines/urls.py
```

Current constraints:

* Only `interval` values: `"1s"` and `"1m"`
* Fixed `limit = 1000`

Given `(symbol, interval, start_ms, end_ms)`:

1. Convert `interval` → milliseconds.
2. Compute `span = interval_ms * limit`.
3. Walk from `start_ms` to `end_ms` in steps of `span`, generating `(slice_start, slice_end)` windows.
4. Build a `/api/v3/klines` URL for each window.

This yields your **ground truth universe of URLs** (or `KlineSliceKey`s) for completeness.

---

## Completeness Logic

Found in:

```text
qlir/data/sources/binance/completeness/registry.py
```

The registry is responsible for answering:

* “Which slices **should** exist?”
* “Which slices **do** exist and are `ok`?”
* “Which slices are `missing` or `failed` and need work?”

Typical flow:

```python
expected_slices = compute_expected_slices(
    symbol, interval, min_start_ms, max_end_ms, limit=1000, slice_iter=generate_kline_slices
)

known_statuses = load_from_manifest(...)  # composite_key -> SliceStatus

missing_slices = compute_missing_slices(expected_slices, known_statuses)
```

The worker then iterates `missing_slices` and fetches each one until there are none left.

The completeness layer is designed so that switching from JSON → SQLite (or another backend) only requires changing the “load/save status” helpers, not the worker logic.

---

## Workers

Each endpoint has its own worker module, e.g.:

```text
qlir/data/sources/binance/endpoints/klines/worker.py
```

Responsibilities:

* Determine `(min_start_ms, max_end_ms)` for the symbol/interval set.
* Use `urls.generate_kline_slices()` to enumerate slices.
* Use `completeness.registry` to find missing/failed slices.
* Call `fetch.fetch_and_persist_slice()` for each slice, with exponential backoff on failures.
* Update statuses in `manifest.json`.

`server.py` decides which workers to run and how (sequential, threads, async, etc.).

---

## Extending the Module

To add a new Binance endpoint:

```text
qlir/data/sources/binance/endpoints/<endpoint>/
  __init__.py
  model.py    # slice model + statuses
  urls.py     # how to enumerate requests (by time, ID, etc.)
  fetch.py    # call Binance, use qlir.io.writer
  worker.py   # completeness loop
```

Then, register/start the new worker in `server.py`.

Each endpoint stays self-contained and follows the same pattern:

> generate → diff → fetch → persist

---

## Future Upgrade Paths

This layout supports future migration to:

* **SQLite** (better concurrency & queries over slices)
* **DuckDB / Parquet** (for analytical workloads)
* **Object storage** (S3, GCS, MinIO) for raw payloads

Nothing in the current design blocks these upgrades.
