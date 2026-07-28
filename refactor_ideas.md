
## 1. Split QLIR conceptually into **core** and **research**

Right now everything is “QLIR.” In your head it’s actually:

* **QLIR-core** → identity + catalog
* **QLIR-research** → indicators, stats, pipelines, disk IO, etc.

You don’t have to make two repos immediately; start with two **subpackages**:

```text
qlir/
  core/
    __init__.py
    instruments.py       # CanonicalInstrument, InstrumentMeta
    timefreq.py          # TimeFreq
    datasource.py        # DataSourceKind, DataTier, DataSource, DataSourceSpec
    symbol_map_base.py   # BaseSymbolMap
    universes.py         # MAJORS, ALTCOINS, etc.
  research/
    indicators/
    pipelines/
    io/
    stats/
```

Then:

* **SPL depends only on `qlir.core`**
* **QLIR’s research code depends on `qlir.core` too**
* **No one in SPL imports `qlir.research.*`**

That gives you:

* Shared naming + identity
* Minimal coupling
* Clear boundaries

---

## 2. What SPL is allowed to import

From SPL’s point of view, the ONLY things it should touch are:

```python
from qlir.core.instruments import CanonicalInstrument
from qlir.core.timefreq import TimeFreq
from qlir.core.datasource import DataSource, DataTier
from qlir.core.symbol_map_base import BaseSymbolMap
from qlir.core.universes import MAJORS, ALTCOINS, ...
```

SPL then uses those to:

* validate config (`execution_source`, `instruments`, `universe`)
* pick an execution venue (`supports_execution`)
* map canonical → venue ids for its adapters
* log consistent instrument names
* maybe choose risk profiles per canonical instrument

But SPL itself (the library) **does not** do:

```python
from qlir.research.indicators import atr
from qlir.research.io import candles_from_disk_or_network
```

(SPL strategy execution projects will **MOST DEFINITELY** use this)

---

## 3. Think of `qlir.core` as a tiny “contract” library

Mentally:

* `qlir.core` = **contract** between all parts of the system
* `qlir.research` = one consumer of that contract
* `spl` = another consumer of that contract

If you ever want to go harder on decoupling, you can literally extract it:

```text
tradesys_core/
  instruments.py
  timefreq.py
  datasource.py
  symbol_map_base.py
  universes.py

qlir/        -> depends on tradesys_core
spl/         -> depends on tradesys_core
```

But you don’t need to start there. Just keep `qlir.core` clean and self-contained, and you can extract it later without pain.

---

## 4. Where `supports_execution` fits in this picture

This helps lock the mental model:

* `supports_execution` lives on **`DataSourceSpec` in `qlir.core.datasource`**
* SPL imports that and does:

  ```python
  src = DataSource.DRIFT  # from qlir.core
  if not src.supports_execution:
      raise ValueError("Invalid execution source")
  ```

QLIR itself never uses `supports_execution` for anything.
It just **declares** it in the catalog.

SPL is the one **enforcing** it.

So the coupling is:

* SPL → `qlir.core.datasource` (tiny, stable)
* Not SPL → `qlir.research` (big, unstable)

That’s good coupling.

---

## 5. Concrete example of “minimal coupling” usage

SPL config:

```toml
[spl.engine]
execution_source = "drift"
instruments = ["SOL_PERP", "BTC_PERP"]
```

SPL bootstrap:

```python
from qlir.core.datasource import DataSource
from qlir.core.instruments import CanonicalInstrument

src = DataSource[config.execution_source.upper()]

if not src.supports_execution:
    raise ValueError(f"{src.name} cannot be used for execution")

instruments = [
    CanonicalInstrument[name]
    for name in config.instruments
]
```

That’s it. SPL doesn’t know about QLIR pipelines, candles, indicators — just the **vocabulary**.

---

## 6. Summary: how to get what you want

You’re aiming for:

> “SPL can leverage the naming and world model QLIR pins down, **without** SPL being coupled to QLIR’s internals.”

You get that by:

1. Making a small **`qlir.core`** subpackage that only handles:

   * instruments
   * timefreq
   * datasource + tiers + supports_execution
   * symbol map base
   * universes
2. Ensuring SPL **only imports from `qlir.core`**
   (never `qlir.research`).
3. Treating `qlir.core` as a tiny shared contract that could later be its own package (`tradesys_core`).

That way SPL *does* leverage the naming,
but the dependency surface area stays small and clean.

---
==============================================
==============================================

# Analysis Server Performance

Two **separate** efforts, different branches, different timelines. Do **not**
merge them into one branch — they answer different questions and the second one
needs to be diffable against `main`.

Both must stay in feature branches and stay **unmerged** until they can be
validated against a **real dataset** (we don't have the data loaded yet).

---
==============================================

## Branch 1 (near-term, quick, merged sooner): ETL — full vs incremental

### The problem (grounded in current code)

Per loop, `analysis_server/server.py:main()` does, in order:

1. `get_clean_data()` → reads N parquet files off disk **+** runs `clean_data()`
   (the full `validate_candles` DQ pass: dedup, gap detection, freq inference,
   datetime index build) — `io/load_clean_data.py` + `etl/pipelines/first_pipeline.py`.
2. staleness alert check
3. **watermark gate**: `if data_ts <= last_processed_ts: sleep; continue`
   (`server.py`, Phase 2).

The ETL in step 1 runs **before** the watermark gate in step 3. So every loop
(default `POLL_INTERVAL_SEC = 15`) pays the full read + clean cost **even when
there is no new data** and the loop is about to bail. And it re-cleans
overlapping data every loop.

Today this is softened by `LAST_N_FILES = 5` (we re-clean the last 5 files, not
the whole set). The pathological case is the `last_n_files = 0` full-load path
(`load_clean_data.py` → `union_file_datasets` over the whole dir), which re-reads
and re-cleans the **entire** dataset every loop.

Note: the *analysis* itself is already cheap — triggers only read `df.iloc[-1]`
(`_last_row`). The recompute cost is in `clean_data` and in
`materialize_required_dfs` (rebuilds every derived DF over the whole `base_df`
each loop). This branch targets the ETL/clean cost; derived-DF caching is a
later, separate concern.

### The plan

Introduce one **strategy knob**, not a pile of booleans:

- `ETL_MODE = full_each_loop | incremental`
  - `full_each_loop` (default, == current behavior) — simple, obviously correct,
    fine for 1m low-freq. **This is also the test oracle.**
  - `incremental` — hold the cleaned DataFrame in memory across loops; on new
    data, ETL only the **delta** (rows since watermark) plus a small **overlap
    seam** at the tail, then append/stitch. Cold start does one full ETL.
- Keep `last_n_files` as the orthogonal *load-scope* knob it already is.
- Optional safe first slice: a **cheap freshness gate before the ETL** — stat
  `head.parquet` mtime, or read just the max timestamp / row count — and
  sleep+continue when it's not newer than `last_processed_ts`. Removes the
  wasted ETL on idle loops on its own; low risk; testable without real data.

### The one correctness catch (this is the crux)

`validate_candles` does dedup + gap detection, which are **stateful at the
boundary**: a gap that spans the last cached row and the first new row must
still be caught. So incremental can't clean the new rows in isolation — it needs
to re-clean a small overlap seam and stitch. That seam is where incremental
could silently diverge from the full pass.

Mitigation = the config *is* the test harness: run `full_each_loop` and
`incremental` on the same input and assert **identical output, row-for-row**.
Keeping full mode around makes incremental falsifiable — which matters
precisely because we can't run real-data tests yet.

Expected payoff: **major** speed win (removes redundant per-loop work).

---
==============================================

## Branch 2 (later, deferred): Polars vs Pandas

Keep this on its **own branch, parallel to `main`**, separate from the ETL
branch. The whole point is to be able to run **both** implementations and
compare, so it must be diffable against main.

### Goals

- Run the Polars and Pandas paths **side by side** and compare:
  - **correctness parity** — identical (or explainably-equivalent) outputs, and
  - **performance** — how much faster Polars actually is.
- Eventually make the engine a **configurable choice** (pandas vs polars) — but
  **avoid adding too much complexity** for now. Start minimal; only generalize
  the engine seam if the benchmark justifies it.

### Open question this branch exists to answer

Unknown how big the Polars win is — it depends heavily on **dataset size**. The
ETL branch removes *redundant* work (a structural win); Polars lowers the
**constant factor** of the work that remains (read + DQ + derived-DF builds).
They're orthogonal and compound — neither substitutes for the other. Do the ETL
branch first (bigger, more certain win); layer Polars on the read/ETL hot path
afterward.

### Deliverable (deferred until real data)

- Benchmark **on real datasets** across a range of sizes (small → large parquet
  chunk counts).
- **Publish the comparison data** (perf + parity). Saved for later.


