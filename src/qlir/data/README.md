# **`data/` Module**

This package is the **data identity and transformation layer** of QLIR/SPL-Trader.

It defines:

* canonical dataset naming (but not reading/writing them)
* canonical disk layout conventions (paths, not I/O)
* instrument & resolution identity
* source-specific fetchers + normalization
* the disk-fast-path loader (which *plans* I/O but delegates execution)
* resampling logic (pure transformations)

It explicitly **does not** perform file system I/O — that lives in the sibling `io/` module.

---

# **Top-Level Structure**

```
data/
  core/
  resampling/
  sources/
```

---

# **`core/` — Canonical Names, Paths, and Identity Helpers**

These modules define the **stable rules for identity** in QLIR.
They never talk to files, never perform fetches, and never do I/O.

---

## **✔ `core/naming.py` — Canonical Filename Contract**

Defines *how datasets are named*, independent of where they live.

### Responsibilities

* canonical candle filename pattern:

  ```
  <instrument_id>_<resolution>.parquet
  ```
* resolution string mapping: `TimeFreq → "1m", "5m", "1h", "1D"`
* basic validation helpers (e.g., `is_canonical_candle_name`)
* canonical metadata key schema, used by:

  * Parquet embedding (`parquet_metadata()`)
  * sidecar JSON metadata (`sidecar_metadata()`)

### Not responsible for:

* deciding directory structure (see `core.paths`)
* reading or writing (`io/` handles that)
* inferring names from unknown files (`core.infer`)
* datasources & network logic (`sources/`)

This file is the **single source of truth** for dataset naming rules.

---

## **✔ `core/paths.py` — Canonical Disk Layout (Paths Only)**

Defines *where datasets live*, but does not read/write anything.

### Responsibilities

* resolve data root (default: `~/qlir_data`)
* build canonical paths:

  ```
  <root>/<datasource>/<instrument>_<resolution>.parquet
  ```
* provide helper functions:

  * `datasource_dir(ds)`
  * `candles_path(instrument, resolution, ds)`
  * optional directory creation helper (without writing the actual dataset)

### Not responsible for:

* reading the dataset
* writing files
* JSON sidecar creation
  (all done in **`io/`**)

---

## **✔ `core/instruments.py` — Canonical Instrument IDs**

Defines stable instrument identifiers for research:

* `SOL_PERP`
* `BTC_PERP`
* `SOL_SPOT_USDC`
* etc.

Used across:

* filename construction
* datasource symbol mapping
* loader
* resampling

---

## **✔ `core/infer.py` — Optional Reverse Engineering Helpers**

These utilities help you identify unknown datasets by inspecting:

1. parquet embedded metadata
2. `.meta.json` sidecar
3. canonical filename pattern
4. columns (rare fallback)

Only used for debugging, importing external files, or exploratory analysis.

Not part of normal workflow.

---

# **`resampling/` — Pure Transformations**

This package operates **only on canonical OHLCV frames**.
It has no notion of:

* datasources
* paths
* disk layout
* upstream symbols
* metadata

### Responsibilities

* TimeFreq aggregation
* OHLCV resampling logic
* partial/incomplete bar handling
* higher-resolution → lower-resolution transformations

---

# **`sources/` — Datasource Fetching, Normalization & Loading**

This package handles *the data acquisition pipeline*:

* mapping canonical instruments → venue symbols
* fetching raw candles
* normalizing venue-specific quirks into canonical frames
* coordinating the disk-fast-path vs network fetch

It does not perform file writes — it prepares data for the **`io/`** module to persist.

---

## **`sources/types.py`**

Defines the `DataSource` enum:

```
DRIFT
HELIUS
KAIKO
MOCK
```

(Disk is not a datasource — it is an optimization.)

---

## **`sources/registry.py`**

Venue metadata (`CandleSpec`):

* timestamp label (start/end)
* column alias lists
* resolution→offset rules
* rolling last bar behavior
* volume preference (base/quote)
* timezone for display

Each datasource keeps its quirks *here*.

---

## **Datasource Subpackages**

Each datasource gets its own folder:

```
sources/drift/
sources/helius/
sources/kaiko/
sources/mock/
```

Each containing:

### `symbol_map.py`

Maps **canonical instrument IDs** → **upstream venue symbols**
e.g. `"sol-perp" → "SOL-PERP"`.

### `fetch.py`

Knows how to call the datasource API.

### `normalize.py`

Canonicalizes venue-specific columns/timestamps into OHLCV format.

---

## **`sources/loader.py` — The Disk-Fast-Path Orchestrator**

This is the **entrypoint** for anything that wants “candles for X at Y resolution”.

Workflow:

```
1. Compute canonical path via core.paths
2. If file exists:
       → load from disk (delegates to io/)
3. Else:
       → require datasource
       → map canonical → upstream symbol
       → fetch raw candles
       → normalize into canonical frame
       → return frame (most funcs will ask for io/ to persist, including writing temp files before creating/appending to the actual dataset that the disk loads for analyses)
```

Important:
`loader.py` calls **io/** for all I/O operations.

---

# **Division of Responsibility (Summary)**

### `data/core`

**Identity**:
what things *are called* and how their paths are formed.

### `data/sources`

**Acquisition**:
how to get data from network vs disk, how to canonicalize it.

### `data/resampling`

**Transformation**:
how to reshuffle or aggregate canonical data.

### `io/`

**Input/Output**:
reading, writing, metadata sidecars, parquet handling, caching.
(This is a separate top-level module.)

All layers depend **down** (core → sources → IO), never horizontally.
