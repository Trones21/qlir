# Gap Materialization & Fill Policies

This module provides **explicit, phase-separated handling of missing time and missing values** in OHLC-style time series data.

The core principle is:

> **Missing time ≠ missing prices**

Time is continuous in the real world. Markets are not.

---

## Design Overview

The gap pipeline is intentionally split into **two distinct phases**:

### 1. Time materialization

Materialize *elapsed time* on a fixed wall-clock grid.

* Creates rows for missing timestamps
* Marks them explicitly
* Makes **no semantic claims** about price

### 2. Value materialization (fill policies)

Given materialized time, apply **explicit, declared assumptions** to generate synthetic OHLC values.

---

## Phase 1: Materialize Missing Rows

```python
from transform.gaps.materialization.materialize_missing_rows import (
    materialize_missing_rows,
)

df = materialize_missing_rows(
    df,
    interval_s=60,  # one row per real-world minute
)
```

### What this does

* Ensures a dense `DatetimeIndex` at the given interval
* Inserts rows where time passed but price was not observed
* Adds a marker column:

```text
row_materialized == True   → time passed, price missing
row_materialized == False  → observed data
```

### What this does *not* do

* ❌ fill prices
* ❌ interpolate values
* ❌ infer why gaps exist (overnight, weekend, outage, halt)

At this point, **time is correct**, but values may be missing.

---

## Phase 2: Apply a Fill Policy

Once time is materialized, you may choose to materialize values using a **fill policy**.

```python
from transform.gaps.materialization.apply_fill_policy import apply_fill_policy
from transform.policy.constant import ConstantFillPolicy

df = apply_fill_policy(
    df,
    interval_s=60,
    policy=ConstantFillPolicy(),
)
```

### Preconditions

* DataFrame must be indexed by **bar-open timestamps**
* `materialize_missing_rows` must already have been run

If these are violated, the function fails loudly.

---

## Example: End-to-End

### Input (sparse data)

```text
timestamp              open   high   low   close
------------------------------------------------
2024-01-01 10:00:00    100    102    99    101
2024-01-01 10:03:00    103    105    102   104
```

### After time materialization

```text
timestamp              open   high   low   close   row_materialized
------------------------------------------------------------------
10:00                  100    102    99    101     False
10:01                  NaN    NaN    NaN   NaN     True
10:02                  NaN    NaN    NaN   NaN     True
10:03                  103    105    102   104     False
```

### After constant fill policy

```text
timestamp              open   high   low   close   is_synthetic  fill_policy
----------------------------------------------------------------------------
10:01                  101    101    101   101     True          constant
10:02                  101    101    101   101     True          constant
```

Observed rows remain untouched.

---

## Fill Policies

Fill policies are **explicit assumptions** about how price *might* have evolved while unobserved.

### ConstantFillPolicy

```python
from transform.policy.constant import ConstantFillPolicy

policy = ConstantFillPolicy()
```

* Carries forward the previous close
* Zero volatility
* Deterministic
* Useful as a baseline / control

---

### WindowedLinearFillPolicy

```python
from transform.policy.windowed_linear import WindowedLinearFillPolicy

policy = WindowedLinearFillPolicy(
    context_window_per_side=5,
    vol_scale=1.0,
)
```

* Linearly interpolates close between boundaries
* Uses **local real-candle context** on each side of the gap
* Derives volatility, trend, or other metrics from context
* Deterministic (no randomness)

The context window is **general local context**, not just for volatility.

---

## Key Guarantees

This module guarantees:

* No silent extrapolation
* No overwriting of observed data
* Explicit tagging of synthetic rows
* Deterministic, reproducible behavior
* Clear separation of *time* vs *value* assumptions

If prices are invented, they are:

* explicitly marked
* attributable to a named policy
* auditable

---

## Mental Model (TL;DR)

```text
materialize_missing_rows
        ↓
explicit gaps in time
        ↓
apply_fill_policy
        ↓
explicit assumptions about price
```

You always know:

* **where time passed**
* **where prices were observed**
* **where prices were assumed**
* **under what policy**
