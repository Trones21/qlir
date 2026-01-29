# qlir.perf

Performance and memory instrumentation utilities for QLIR.

This module provides **lightweight, explicit observability** for:
- DataFrame memory growth
- Process RSS changes
- Wall-clock runtime for key operations

The goal is not micro-optimization, but **intuition building**:
> “How big is this dataset?”  
> “What did this transformation actually cost?”  
> “Where do memory spikes occur in the pipeline?”

---

## Design philosophy

- **No hidden behavior**  
  Instrumentation is explicit at call sites. Nothing logs unless you ask it to.

- **Low overhead**  
  Measurements are safe to use at step boundaries and feature construction points.
  Do not place instrumentation in per-row hot loops.

- **Calibration over noise**  
  INFO logs teach stable expectations.  
  DEBUG logs explain mechanics when investigating.

- **RSS + DataFrame size**  
  Both are captured, because they answer different questions:
  - DataFrame bytes → logical data footprint
  - RSS → actual process pressure seen by the kernel

---

## Modules

### `df_copy.py`

Measured wrapper around `DataFrame.copy()`.

```python
df2, ev = df_copy_measured(
    df,
    deep=True,
    label="with_candle_relation_mece",
)
````

Captures:

* DataFrame memory before / after
* Process RSS before / after
* Elapsed wall-clock time

Returns the copied DataFrame and a `MemoryEvent`.

---

### `memory_event.py`

Defines the immutable `MemoryEvent` value object.

A `MemoryEvent` contains:

* optional label
* DataFrame bytes before / after
* RSS before / after
* elapsed seconds

Convenience properties:

* `df_delta_bytes`
* `rss_delta_bytes`

Includes helpers for human-readable byte formatting.

---

### `logging.py`

Formatting and logging helpers for `MemoryEvent`.

Two verbosity levels are intentionally supported:

#### DEBUG — mechanics

Detailed, step-level information for investigation.

Example:

```text
[mem] with_candle_relation_mece |
df: 8.42 GB → 8.71 GB (Δ 0.29 GB) |
rss: 19.4 GB → 19.9 GB (Δ 0.5 GB) |
0.312s
```

Use for:

* individual feature construction
* helper transforms
* internal copies

#### INFO — calibration

High-level, stable signals meant to build intuition.

Example:

```text
[mem] legs_bundle | rss Δ +4.8 GB (df Δ +3.9 GB)
```

Use for:

* base dataset load
* major column bundles
* pipeline phase boundaries
* peak memory summaries

---

## Recommended usage pattern

At call sites:

```python
out, ev = df_copy_measured(df, label="with_legs_bundle")
log_memory_debug(ev=ev, log=log)
```

Promote to INFO **only** when crossing a semantic boundary:

```python
out, ev = add_legs_bundle(df)
log_memory_info(ev=ev, log=log)
```

Rule of thumb:

> If the number should become “normal” to the user → INFO
> If the number is only useful when debugging → DEBUG

---

## What this module is *not*

* Not a profiler
* Not a tracing framework
* Not a memory manager
* Not a streaming substitute

It exists to make reality visible — not to hide or abstract it away.

---

## Why this matters

QLIR operates on:

* wide, derived DataFrames
* long historical time series
* stateful transformations with cross-row dependencies

In this environment, **knowing where memory goes** is more valuable than prematurely optimizing it.

This module makes those costs explicit, measurable, and understandable.
