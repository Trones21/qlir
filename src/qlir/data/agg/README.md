# Aggregation Layer (agg)

## Purpose

The **agg layer** is responsible for **materializing raw slices into a scan-efficient, columnar dataset** without altering their semantic meaning.

It exists to bridge the gap between:

* **raw** — immutable, slice-addressed request/response artifacts optimized for correctness and provenance
* **downstream consumers** — analytics, validation, and research code that expect fast range scans and columnar access

The agg layer performs **structural aggregation only**.
It does **not** validate, clean, resample, or interpret data.

# location/structure 
```text
qlir_data/
└── binance/
    └── klines/
        ├── raw/                                     # Source-of-truth ingestion layer(s)
        │   └── BTCUSDT/
        │       └── 1m/
        │           ├── limit=500/                  # Specifies slice size                   
        │           │   ├── manifest.json           # Raw limit=500 slice ledger (immutable)
        │           │   └── responses/
        │           │       ├── e901567f.json
        │           │       └── ...
        │           │
        │           └── limit=1000/                 # Specifies slice size 
        │               ├── manifest.json           # Raw limit=1000 slice ledger (immutable)
        │               └── responses/
        │                   ├── 91bcde77.json
        │                   └── ...
        │
        └── agg/                                    # Columnar materialization layer
            └── BTCUSDT/
                └── 1m/
                    ├── limit=500/                  # Specific slice size - Agg should only pull from corresponding raw layer (limit=500)
                    │   ├── manifest.json           # agg dataset index
                    │   └── parts/
                    │       ├── part-000001.parquet
                    │       └── ...
                    │
                    └── limit=1000/
                        ├── manifest.json
                        └── parts/
                            ├── part-000001.parquet
                            └── ...

```

Note: In practice it is unlikely that you will store multiple slice sizes (500, 1000), but we show two here because pulling different slice sizes **is possible**, and this **will** result in different raw manifests (composite key hashsets). Therefore its best for the folder structure to be explicit as well.  
---

## What agg *is*

Agg is a **packer**, not a transformer.

Specifically, it:

* Observes the raw manifest
* Selects successfully fetched slices
* Bundles multiple slices into immutable Parquet files
* Maintains a dataset-level manifest describing exactly which slices have been materialized

The output is a **Parquet dataset** (many immutable files) that behaves as a single logical table when queried.

---

## What agg is *not*

Agg deliberately does **not**:

* Enforce completeness
* Assume contiguous time ranges
* Detect or repair gaps
* Interpret partial candles
* Mutate or annotate the raw layer
* Make correctness judgments

Those responsibilities belong to later layers (`clean`, `canonical`, etc.).

---

## Design principles

The agg layer follows a small number of strict principles:

* **Raw is immutable**
  Agg never writes back to raw or marks slices as “used”.

* **Slice identity is authoritative**
  Slice hashes are the only unit of identity and provenance.

* **Append-only materialization**
  Parquet files are written once and never modified.

* **Manifest-driven truth**
  The agg manifest is the sole source of truth for what has been materialized.

* **Crash safety by construction**
  Parquet writes and manifest updates form an atomic commit boundary.

---

## Why a long-running process

Agg is implemented as a **long-running daemon**, similar to ingestion, but purely reactive:

* It does not initiate work — it responds to new raw slices becoming available
* It naturally applies backpressure
* It avoids scheduling gaps and missed work
* It keeps downstream datasets continuously materialized

Agg is **availability-driven**, not time-window-driven.

---

## Mental model

Think of agg as:

> An append-only, manifest-indexed segment writer that packs verified raw facts into scan-optimized containers.

It does not decide *what the data means* — only *how it is stored*.

---

# Core agg loop (conceptual)

Here’s the simplest correct loop:

```text
loop forever:
    load raw manifest
    load agg manifest

    slices = get_slices_needing_to_be_aggregated(raw manifest, agg_manifest)

    if fewer than N slices:
        sleep
        continue

    select next N slices (oldest)
    materialize parquet part
    write agg manifest entry

    continue
```

for determining the slices that need to be aggregated:
```python
def get_slices_needing_to_be_aggregated(raw_manifest, agg_manifest):
    eligible = {
        s for s in raw_manifest.slices
        if raw.status == OK
    }

    already_used = {
        s for s in agg_manifest.all_slice_hashes()
    }

    todo = eligible - already_used
```

# Agg Manifest Structure/Example

```json
{
  "dataset": {
    "source": "binance",
    "kind": "klines",
    "symbol": "BTCUSDT",
    "interval": "1m"
  },
  "parts": [
    {
      "part": "part-000001.parquet",
      "slice_hashes": [
        "e901567f...",
        "a14c33be..."
      ],
      "row_count": 50000,
      "min_open_time": 1610462400000,
      "max_open_time": 1610894340000,
      "created_at": "2025-12-20T15:12:09Z"
    }
  ]
}
```

---

## Selection strategy

### Oldest first (recommended)

* Sort by slice start_ms
* Take earliest un-aggregated slices

This keeps parts roughly time-ordered which improves locality without assuming contiguity


#### Rebuildability and temporal locality

The aggregation layer is intentionally **rebuildable**.

Because agg:

* Never mutates the raw layer
* Derives all state from the raw manifest
* Tracks slice usage exclusively via its own manifest

…the entire agg dataset can be **safely deleted and regenerated** at any time.

This property enables future improvements without changing upstream ingestion.

#### Temporal locality as an optimization, not an assumption

The default selection strategy (“oldest un-aggregated slices first”) has two important effects:

1. **Progressive materialization**
   Early historical slices are packed first, while newer slices naturally trail.

2. **Recoverable locality**
   If the agg layer is rebuilt after the raw dataset is complete, slices will be re-packed in near-temporal order, improving locality within Parquet parts.

Importantly, agg **does not assume** temporal contiguity during normal operation.
Temporal locality is treated as an **emergent optimization**, not a correctness requirement.

---

### Why this matters

This design means:

* Early, incremental runs prioritize availability
* Late rebuilds can prioritize layout quality
* No permanent decisions are locked into file structure
* Locality can improve over time without changing semantics

In other words, **correctness comes first, layout second** — and layout is always recoverable.

---

# Aggregation contract

Agg guarantees:

1. Each raw slice is aggregated **at most once**
2. Aggregated parquet contains **only rows from its slices**
3. Rows are **sorted by open_time**
4. Schema is consistent across parts
5. Files are immutable

Agg does **not** guarantee:

* Completeness
* Contiguity
* Absence of gaps
* Final correctness

That’s a later layer’s job.

---

# Handling failures (this is where daemons go wrong)

Agg failures should be **local and non-blocking**.

### If a slice fails to load / parse

* Mark slice key (in manifest) as `agg_failed` (with error)
* Skip it
* Do *not* block other slices

### If parquet write fails

* Do not mark slices as aggregated
* Retry later
* Temp files cleaned up

### If process crashes mid-write

* Atomic rename prevents partial files
* Manifest remains consistent

Agg should be **crash-safe by construction**.

---

# 8. Manifest shape (minimal but sufficient)

Agg manifest entries should look like:

```json
{
  "part": "part-000042.parquet",
  "slice_hashes": [
    "e901567f...",
    "a14c33be...",
    "9fa0021c..."
  ],
  "row_count": 48721,
  "min_open_time": 1610462400000,
  "max_open_time": 1610894340000,
  "created_at": "2025-12-20T15:12:09Z"
}
```

Important:

* Slice hashes are **the truth**
* min//max is not saying that this file contains the full range from min tomax. min/max are *hints* (so parquest engine can query effectively)
    * 
* part id is just a label

---

# 9. Sleeping strategy (don’t overthink)

Simple rules:

* If no new eligible slices → sleep longer
* If work was done → loop immediately
* If partial batch (< N) → short sleep

Something like:

* Idle: 30–60s
* Partial batch: 5–10s
* Active: no sleep

That’s plenty.



