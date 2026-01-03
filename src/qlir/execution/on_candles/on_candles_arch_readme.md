## Two Representations: Row-Aligned vs Path-Aligned

QLIR uses two complementary execution representations:

### `on_summary` (path-aligned)

* one row per contiguous `True` path
* “collapsed-time” view
* used for distributions/statistics

### `on_candles` (row-aligned)

* **same cardinality as the original candle frame**
* “trajectory” view
* used for inspection and for generating features/reducers

`on_candles` is intentionally **not** a final output format.
It is a **rich intermediate representation**.

---

## Core Semantics of `on_candles`

For each contiguous `True` segment (a “path”):

* choose an **entry anchor** (e.g., first candle’s high for worst-case long)
* for every row inside the path:

  * compute “exit if we closed now”
  * compute pnl relative to entry anchor
  * optionally compute running MAE/MFE inside the path

Outside paths (`cond=False`):

* execution columns are typically `NaN` (or left unchanged), depending on policy

---

## Why Validation Is Mandatory

Row-aligned candle execution assumes:

* wall-clock structure
* fixed interval
* no gaps

Without these invariants, a “trajectory” can silently skip time, which makes:

* path length
* excursions
* exits
* MAE/MFE
  fundamentally incorrect.

Therefore, candle validation is enforced via a decorator.

---

## Why `interval_s` Is Call-Time

The interval is market context (1m, 3m, 5m, …) and varies per run.
Validation must use the caller’s `interval_s`, so decorators read it dynamically from call kwargs.

---

## Condition Paths Still Exist (But Output Isn’t Reduced)

Even though `on_candles` returns row-aligned output, it still relies on the concept of contiguous paths, typically via:

* a boolean condition column (`cond`)
* internal path IDs used for grouping/running computations

These IDs may be optionally surfaced for debugging.

---

## Summary

* `on_candles` outputs row-aligned trajectories
* `on_summary` outputs path-aligned summaries
* both rely on the same underlying notion of contiguous condition paths
* validation is a correctness boundary, not a convenience check
