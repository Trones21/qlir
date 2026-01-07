# QLIR Column Semantics Architecture

## Goal

QLIR needs “row-truth” explainability:

- When a function creates new column(s), it must be explicit about **which rows were read** and **which row was written**.
- Intermediate columns may be created and later dropped (e.g., `keep="final"`), but their derivation truth must remain auditable.

This system provides a lightweight provenance layer without requiring pandas expertise.

---

## Core Concepts

### ColumnDerivationSpec

A `ColumnDerivationSpec` describes how a single derived column value at row `i` is computed in row terms:

- `read_rows=(lo, hi)` are inclusive offsets relative to `i`
  - `(-2, 0)` => uses rows `[i-2 .. i]` (self-inclusive rolling window=3)
  - `(-14, -1)` => uses rows `[i-14 .. i-1]` (row-exclusive rolling window=14)
  - `(-1, -1)` => uses row `[i-1]` (lag)

It also records:
- `base_cols` (inputs)
- `scope` (`output` vs `intermediate`)
- `self_inclusive` (explicit disambiguation)
- optional `grouping` boundary (e.g. per-asset rolling)

---

## Lifetimes vs Visibility

Columns have a lifecycle independent of whether they appear in the returned DataFrame:

- `created`: column was computed
- `dropped`: column removed from output (e.g., `keep="final"`)

Dropping affects visibility only; it does not erase derivation truth.

> Rule: columns may be dropped from the DataFrame, but never from the derivation history.

---

## DerivationContext

`DerivationContext` is a lightweight collector for:

- `(col, spec)` records for created columns
- lifecycle events (created/dropped)

It is installed using:

```python
with derivation_scope() as ctx:
    df, out_col = my_indicator(...)
```

Nested calls share the same context, enabling end-to-end provenance across composed pipelines.

---

## Decorator Contract: @new_col_func

Any function that “creates a new column” should use `@new_col_func(...)`.

Contract:

* function returns `(df, out_cols)`
* `out_cols` can be:

  * `str` (single col)
  * `list[str]` / `tuple[str]` (multiple cols)
  * `dict[str, str]` mapping roles to columns (preferred for multi-col funcs)

Specs can be:

* a single `ColumnDerivationSpec` (single output col)
* a list of specs aligned positionally (multi-col)
* a dict of role -> spec aligned with returned role mapping

The decorator:

* logs an explicit row-semantic message per created column
* records the spec in an active `DerivationContext` (if present)

---

## Dropping Columns While Preserving History

Use `qlir.core.semantics.ops.drop_cols(...)` instead of `df.drop(...)` when dropping intermediate columns so the context records:

* which columns were dropped
* why (e.g., `keep="final"`)

Example:

```python
if keep == "final":
    df = drop_cols(df, [tmp1, tmp2], reason="keep=final (drop intermediates)")
```


## ✅ Files

### 1) `qlir/core/semantics/row_derivation.py`

### 2) `qlir/core/semantics/context.py`

### 3) `qlir/core/semantics/explain.py`

### 4) `qlir/core/semantics/decorators.py`

This decorator supports:

* **single col** return (`str`)
* **multi cols** return (`list/tuple[str]`)
* **role-mapped** returns (`dict[str, str]`) → best when you have “abs/pct” etc.

### 5) `qlir/core/semantics/ops.py` (tiny helper for “drop but keep history”)
---

## ✅ How you use it

### A) Multi-col creator with roles (best)

```python
from qlir.core.semantics.decorators import new_col_func
from qlir.core.semantics.row_derivation import ColumnDerivationSpec

@new_col_func(
    specs={
        "abs": ColumnDerivationSpec(op="diff", base_cols=("open",), read_rows=(-1, 0), scope="intermediate", self_inclusive=True),
        "pct": ColumnDerivationSpec(op="pct_change", base_cols=("open",), read_rows=(-1, 0), scope="intermediate", self_inclusive=True),
    }
)
def with_diff(...):
    ...
    return df, {"abs": abs_col, "pct": pct_col}
```

### B) “keep=final” dropping intermediates (but still recorded)

```python
from qlir.core.semantics.ops import drop_cols

if keep == "final":
    df = drop_cols(df, [abs_col, pct_col], reason="keep=final (drop intermediates)")
```

This way:

* derivations are logged + recorded as **created**
* drops are recorded as **dropped**
* you never “erase” provenance
