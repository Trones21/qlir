## Purpose

Execution models in `on_candles` operate on **raw candle data** and return **row-aligned execution metrics**.

Unlike `on_summary` (which collapses each condition path to one row), `on_candles` preserves the original candle cardinality and adds columns that show how execution metrics evolve **across time** inside each contiguous condition path.

---

## When to Use `on_candles`

Use `on_candles` when you want:

* a time-series view of PnL/MAE/MFE evolving candle-by-candle
* to visualize execution trajectories inside each contiguous `True` segment
* row-aligned features to feed into later reducers/distributions

If you only need one value per path, use `on_summary`.

---

## Input DataFrame Shape

One row per wall-clock interval (or resampled interval), with time as index.

Example:

```text
index (DatetimeIndex) | open | high | low | close | cond
---------------------|------|------|-----|-------|-----
00:00                | ...  | ...  | ... | ...   | False
00:01                | ...  | ...  | ... | ...   | True
00:02                | ...  | ...  | ... | ...   | True
00:03                | ...  | ...  | ... | ...   | True
00:04                | ...  | ...  | ... | ...   | False
```

---

## Output Shape

**Same number of rows as input.**

The model adds row-aligned columns such as:

* `exec_entry_price` (constant within a path)
* `exec_pnl` (0 at first row of the path; evolves thereafter)
* `exec_mae` / `exec_mfe` (running adverse/favorable excursion inside the path)
* optional: `exec_path_id` (for debugging/visualization)

Example output (conceptual):

```text
index | cond | exec_path_id | exec_entry_price | exec_exit_price | exec_pnl
------|------|--------------|------------------|-----------------|---------
00:00 | F    | NaN          | NaN              | NaN             | NaN
00:01 | T    | 0            | 101.0            | 101.0           | 0.0
00:02 | T    | 0            | 101.0            | 102.2           | 1.2
00:03 | T    | 0            | 101.0            | 100.8           | -0.2
00:04 | F    | NaN          | NaN              | NaN             | NaN
```

Interpretation:

* within each contiguous `True` segment, we anchor entry once
* each row shows “if we exited on *this row*, what would the result be?”

---

## Typical Usage

(Exact function names may differ depending on your final public API; this shows the intended call pattern.)

```python
from qlir.execution.on_candles.execute import execute_candles
from qlir.execution.on_candles.execution_models import CandleExecutionModel

df2 = execute_candles(
    candles=df,
    model=CandleExecutionModel.WORST_ENTRY_EXIT_ON_CLOSE,
    condition_col="cond",
    interval_s=60,
    direction="up",
)
```

---

## Notes

* This module is intended to produce **row-aligned features** that can later be reduced into:

  * path summaries (`on_summary`-like reducers)
  * distributions
  * plots/debug views

---

## Next

For strict invariants, validation, and how the decorator enforces correctness, see:

> `execution/on_candles/ARCHITECTURE.md`
