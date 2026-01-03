## Overview

Execution models in `on_summary` operate on **path-level summaries**, not raw candle data.

They assume that:

* time has already been collapsed
* intrapath dynamics are unavailable
* execution is applied algebraically using summary columns (e.g. `first_*`, `last_*`)

Because of this, **on-summary execution is fast, deterministic, and limited by design**.

---

## End-to-End Flow (Conceptual)

```text
raw wall-clock candles
→ (optional resampling)
→ condition mask
→ condition path IDs
→ path summarization (reducers)
→ execution on summary
```

This README walks through that flow step by step.

---

## 1. Start: Wall-Clock Candle Data (Pre-Summarization)

You begin with a normal candle DataFrame: **one row per wall-clock interval**.

> ⚠️ It does *not* matter if you resampled
> (e.g. 1m → 3m candles), as long as:
>
> * the index represents wall-clock time
> * gaps are consistent across the frame

Example:

```text
index (DatetimeIndex) | open | high | low | close
---------------------|------|------|-----|------
00:00                | 100  | 101  | 99  | 100.5
00:01                | 100.5| 102  | 100 | 101.8
00:02                | 101.8| 103  | 101 | 102.9
00:03                | 102.9| 103  | 101 | 101.2
00:04                | 101.2| 102  | 100 | 100.7
```

This is **not** execution input yet.

---

## 2. Define a Condition Mask

Execution paths are defined by **contiguous runs where a condition is true**.

Example condition:

```python
df["cond"] = df["close"] > df["open"]
```

Result:

```text
index | cond
------|-----
00:00 | True
00:01 | True
00:02 | True
00:03 | False
00:04 | False
00:05 | True
00:06 | True
```

---

## 3. Assign Condition Path IDs

Contiguous `True` segments become **distinct paths**.

Conceptually:

```text
True, True, True   → path_id = 0
False, False       → ignored
True, True         → path_id = 1
```

After assigning IDs:

```text
index | cond | path_id
------|------|--------
00:00 | True | 0
00:01 | True | 0
00:02 | True | 0
00:03 | False| NaN
00:04 | False| NaN
00:05 | True | 1
00:06 | True | 1
```

Only rows with a `path_id` participate in summarization.

---

## 4. Summarize Condition Paths (Reducers)

Each path is reduced to **one row** using path reducers.

This is where time is collapsed.

Example reducer call:

```python
paths = df.reducers.summarize_condition_paths(
    condition_col="cond",
)
```

Resulting DataFrame:

```text
path_id | first_open | first_high | first_low | first_close | last_close | bars
--------|------------|------------|-----------|-------------|------------|-----
0       | 100.0      | 101.0      | 99.0      | 100.5       | 102.9      | 3
1       | 100.7      | 102.0      | 100.0     | 101.2       | 101.9      | 2
```

At this point:

* **one row = one condition path**
* intrapath ordering is gone
* only summary columns remain

This DataFrame is the **input to on-summary execution**.

---

## 5. Execution on Summary

Execution models now operate **purely on summary columns**.

Example: worst entry / exit on close.

```python
from qlir.execution.on_summary.execute import execute_summary
from qlir.execution.on_summary.execution_models import SummaryExecutionModel

executed = execute_summary(
    paths,
    model=SummaryExecutionModel.WORST_ENTRY_EXIT_ON_CLOSE,
    direction="up",
)

```


Output:

```text
path_id | entry_price | exit_price | pnl | pnl_pct
--------|-------------|------------|-----|---------
0       | 101.0       | 102.9      | 1.9 | 0.0188
1       | 102.0       | 101.9      | -0.1| -0.0010
```

---

## What On-Summary Execution *Can* Do

* use `first_*` / `last_*` columns
* compute deterministic entry/exit prices
* produce fast path-level PnL estimates
* support large-scale distribution analysis

---

## What On-Summary Execution *Cannot* Do

* scan candles
* detect intrapath extrema
* simulate fills within a path
* account for timing inside the path

If you need any of the above, use **`execution/on_candles`** instead.

---

## Design Contract (Important)

* on-summary execution **assumes summaries are correct**
* no temporal validation is performed
* misuse is prevented by *structure*, not runtime checks

This is intentional.

---

## Summary

* `on_summary` is for **collapsed-time execution**
* inputs are **path-level rows**
* outputs preserve row count
* execution is algebraic, not simulated

Write summaries carefully — execution will trust them.


## Create your Own Execution Model

```python
from qlir.execution.on_summary.execute import execute_summary
from qlir.execution.on_summary.execution_models import SummaryExecutionModel

def my_execution(paths):
    paths = paths.copy()
    paths["entry_price"] = paths["first_open"]
    paths["exit_price"] = paths["last_close"]
    return paths

executed = execute_summary(
    paths,
    model=SummaryExecutionModel.DIY,
    diy_fn=my_execution,
)