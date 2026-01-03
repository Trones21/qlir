Read the regular readme (for on_summary) for an in depth explanation of a full workflow 

Perfect — that’s actually the **best possible teaching move** 👍
It avoids semantic confusion, aligns exactly with your summarizer, and reinforces the architecture instead of inventing fake use-cases.

Below is a **fully revised DIY README**, end-to-end, that:

* explicitly says *we are recreating a built-in model*
* explains **why** DIY exists even though built-ins do
* shows **how to register**
* shows **how to execute**
* does **not** imply extra summarizer capabilities
* does **not** confuse first/nth candle semantics

You can drop this in as the new DIY article.

---

# DIY Summary Execution Models

## Purpose

This article shows how to **create and register your own summary execution model**.

For clarity and correctness, the example below intentionally **recreates the logic of an existing built-in model** (`WorstEntryExitOnClose`).
The goal is not to invent new behavior, but to demonstrate **how DIY execution works**, end-to-end, using logic that is already well-defined.

---

## What “Summary Execution” Means (Important)

Summary execution operates on **already-collapsed condition paths**.

By this stage:

* each row represents **one condition path**
* time inside the path is no longer available
* all information is encoded in summary columns

As a result, summary execution is fundamentally:

> **row-by-row arithmetic between columns**

There is:

* no scanning
* no fill simulation
* no intrapath timing

DIY execution exists because many execution ideas are naturally expressed at this level.

---

## Why DIY Exists (Even With Built-Ins)

Built-in execution models are:

* canonical
* audited
* closed

DIY execution models are:

* experimental
* user-defined
* reusable across experiments
* not part of the library’s public contract

DIY lets you:

* prototype ideas
* mirror built-ins for experimentation
* tweak logic without forking the library
* share execution logic across notebooks

---

## Example Input (Summarized Paths)

The following DataFrame is the **output of the standard condition path summarizer**:

```text
path_id | first_high | path_min_low | path_max_high | last_close
--------|------------|--------------|---------------|-----------
0       | 101.0      | 99.0         | 103.0         | 102.9
1       | 102.0      | 100.0        | 104.0         | 101.9
```

Each row corresponds to **one contiguous condition path**.

---

## Built-In Logic Being Recreated

The built-in model **WorstEntryExitOnClose** does the following (for `direction="up"`):

* Entry: `first_high`
* Exit: `last_close`
* PnL: `exit - entry`
* MAE: `path_min_low - entry`
* MFE: `path_max_high - entry`

We will recreate this logic exactly as a DIY model.

---

## Writing a DIY Execution Model

```python
def worst_entry_exit_on_close_diy(paths):
    """
    DIY recreation of the built-in WorstEntryExitOnClose model.

    Entry:
        first_high
    Exit:
        last_close
    """
    df = paths.copy()

    entry = df["first_high"]
    exit_ = df["last_close"]

    df["entry_price"] = entry
    df["exit_price"] = exit_
    df["pnl"] = exit_ - entry
    df["mae"] = df["path_min_low"] - entry
    df["mfe"] = df["path_max_high"] - entry

    return df
```

This function:

* operates row-by-row
* uses only summary columns
* returns a DataFrame with the same row count

No decorators or base classes are required.

---

## Registering the DIY Model

DIY execution models are registered into the **DIY summary execution registry**.

```python
from qlir.execution.on_summary.diy import diy

diy.register(
    "worst_entry_exit_on_close_diy",
    worst_entry_exit_on_close_diy,
)
```

Notes:

* Names must be unique within the DIY namespace
* DIY names cannot collide with built-ins
* Registration happens at runtime

---

## Executing the DIY Model

DIY models are executed through the **same public API** as built-ins.

```python
from qlir.execution.on_summary.execute import execute_summary

executed = execute_summary(
    paths,
    model=("diy", "worst_entry_exit_on_close_diy"),
)
```

Conceptually, this corresponds to:

```text
.summary.diy.worst_entry_exit_on_close_diy
```

This explicit namespace prevents ambiguity and accidental misuse.

---

## Verifying Against the Built-In Model

Because this DIY model mirrors a built-in, you can easily compare outputs:

```python
from qlir.execution.on_summary.execution_models import SummaryExecutionModel

builtin = execute_summary(
    paths,
    model=SummaryExecutionModel.WORST_ENTRY_EXIT_ON_CLOSE,
    direction="up",
)

diy_version = execute_summary(
    paths,
    model=("diy", "worst_entry_exit_on_close_diy"),
)
```

The results should match.

This makes DIY useful not just for experimentation, but also for **validation and testing**.

---

## Discovering Available Models

You can list all available summary execution models at runtime:

```python
from qlir.execution.on_summary.execute import (
    list_available_summary_execution_models,
)

list_available_summary_execution_models()
```

Example output:

```python
{
    "builtin": [
        "best_entry_exit_on_close",
        "mid_entry_exit_on_close",
        "worst_entry_exit_on_close",
    ],
    "diy": [
        "worst_entry_exit_on_close_diy",
    ],
}
```

---

## Important Constraints

DIY summary execution models:

* must not depend on intrapath time
* must not assume access to nth candles
* must operate only on provided summary columns

If you need:

* nth open / nth close
* intrapath sequencing
* fill timing assumptions

You must either:

* extend the summarizer (e.g. full pivot reducer), or
* use `execution/on_candles`

---

## Summary

* Summary execution is algebraic, not temporal
* DIY models make this explicit
* Built-ins are closed; DIY is open
* Both are executed through the same safe API
* Recreating built-ins is a valid and useful DIY pattern

DIY is not a workaround — it is a **first-class extension mechanism**.
