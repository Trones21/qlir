Perfect — here’s an **expanded README with concrete usage examples**, written to be skim-friendly but precise.
This should live at:

```
qlir/core/registries/columns/README.md
```

---

# Column registries

This submodule defines **diagnostic registries for semantic DataFrame column keys**.

It provides:

* A **global catalog** of known semantic column keys (QLIR’s column vocabulary)
* **Per-call registries** describing what keys a function actually returned
* **Runtime warnings** when caller expectations do not match callee outputs

This system is intentionally **non-failing by default**.
Its purpose is observability and contract introspection, not enforcement.

---

## Mental model

Think in terms of *symbols*, not columns.

* A **key** is a semantic identifier (e.g. `"last_leg_idx"`)
* A **column name** is a concrete DataFrame implementation detail
* A **registry** is a *receipt* describing what a function produced

This answers questions like:

> “Are you sure this function returns a column called `last_leg_idx`?”

---

## Components

### Global catalog (`ColumnKeyCatalog`)

> “What column keys does QLIR know about *at all*?”

* Lives for the lifetime of the process
* Append-only
* Used to detect typos and vocabulary drift
* Unknown keys **log warnings**, they do not fail

```python
from qlir.core.registries.columns import COLUMN_KEYS

COLUMN_KEYS.register("leg_id")
COLUMN_KEYS.register("last_leg_idx")
COLUMN_KEYS.register_many([
    "leg_len",
    "intra_leg_idx",
])
```

You can add keys gradually as they stabilize across analyses.

---

### Per-call registry (`ColRegistry`)

> “What column keys did *this function call* return?”

* Created by a function
* Returned alongside a DataFrame
* Scoped to a single call
* Used by callers to probe expectations

---

## Usage examples

### 1. Producer (callee): declaring returned keys

```python
from qlir.core.registries.columns import ColRegistry

def mae_up(df: pd.DataFrame, ...) -> tuple[pd.DataFrame, ColRegistry]:
    cols = ColRegistry(owner="sma_14_exec.mae_up")

    cols.declare("leg_id", column="open_sma_14_up_leg_id")
    cols.declare(
        "last_leg_idx",
        column="osma_14_mae_up_pct_sum_last_leg_idx_cum",
    )

    return df, cols
```

Notes:

* Declaring a key that is **not in the global catalog** logs a warning
* This is a signal to the author that the vocabulary may need updating

---

### 2. Consumer (caller): resolving expected keys

```python
df, cols = mae_up(...)

last_leg_idx_col = cols.resolve("last_leg_idx")
if last_leg_idx_col is not None:
    df[last_leg_idx_col]
```

If `"last_leg_idx"`:

* is known globally ✅
* and was returned by this function ✅
  → no warnings

---

### 3. Consumer: probing a key that was not returned

```python
cols.lookup("leg_len")
```

If `"leg_len"`:

* is known globally ✅
* but was not returned by this function ❌

You will see a warning like:

```
WARNING: Column key 'leg_len' was requested but not returned by this call
         (owner=sma_14_exec.mae_up).
         Returned keys: ['leg_id', 'last_leg_idx']
```

This answers:

> “Are you sure this function returns `leg_len`?”

---

### 4. Consumer: probing a completely unknown key

```python
cols.lookup("totally_made_up_key")
```

This produces **two independent signals**:

1. From the global catalog:

   ```
   WARNING: Unknown column key referenced: 'totally_made_up_key'
   ```
2. From the per-call registry:

   ```
   WARNING: Column key 'totally_made_up_key' was requested but not returned
   ```

This strongly suggests a typo or an invalid assumption.

---

### 5. Declaring keys without concrete column bindings

Sometimes a function conceptually returns a key, but the concrete column
is not relevant or not yet known.

```python
cols.declare("leg_len")
```

Later, callers can still probe for existence:

```python
if cols.has("leg_len"):
    ...
```

Or resolve opportunistically:

```python
col = cols.resolve("leg_len")   # returns None if unbound
```

---

### 6. Debugging: dumping registry contents

```python
cols.dump()
```

Example output:

```
INFO: ColRegistry (owner=sma_14_exec.mae_up): 2 keys
INFO:   leg_id                  -> open_sma_14_up_leg_id
INFO:   last_leg_idx             -> osma_14_mae_up_pct_sum_last_leg_idx_cum
```

Useful when tracing complex pipelines.

---

## Design philosophy

This system is designed to be:

* **Exploratory-friendly**
* **Non-rigid**
* **Low-ceremony**
* **Runtime-observable**

It is explicitly **not**:

* a schema system
* a validation framework
* a hard contract enforcer

Warnings are meant to surface:

* drift between caller expectations and callee behavior
* typos in semantic keys
* missing vocabulary registration

Not to stop execution.

---

## When to add keys to the global catalog

Add keys when they become:

* reused across analyses
* stable in meaning
* part of QLIR’s shared vocabulary

Until then, warnings are expected and acceptable.

---

## Summary

* **Catalog** → what QLIR knows about
* **Registry** → what this call returned
* **Lookup** → “are you sure?”
* **Warnings** → signals, not errors

If you find yourself wanting stricter behavior, that should happen
*outside* this module, at higher-level orchestration layers.
