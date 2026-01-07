### `core/ops`

> Column-producing operations for pandas DataFrames.

This directory contains **column-level transforms** that operate on Series values and return new columns.
All operations preserve index alignment and do not introduce relational semantics.

Operations are intentionally split by **temporal dependency**.

---

## `non_temporal`

Operations that depend **only on the value at the same row**.

**Invariant:**

> Output at index *t* depends only on input at index *t*.

Examples:

* absolute value
* sign
* clipping
* scaling

```python
with_abs(df, "x")
with_sign(df, "x")
```
---

## `temporal`

Operations that depend on **other rows in the same column**.

**Invariant:**

> Output at index *t* depends on values at indices ≠ *t*.

Examples:

* shifts
* differences
* returns
* lagged transforms

```python
with_shift(df, "x", periods=1)
with_diff(df, "x")
with_pct_change(df, "x")
```

These operations make time dependence **explicit**.

---

## Relationship to `relations`

Operations in `core/ops` **transform values**.
They do **not** compare values or define predicates.

Relational logic lives in:

* `relations/comparators` → pointwise value relations
* `relations/range_relations` → value ↔ interval semantics
* `relations/crossovers` → temporal relations

If an operation answers a **yes/no question**, it does not belong here.

---

## One-sentence rule

> **If an operation looks at another row, it is temporal.
> If it doesn’t, it is non-temporal.**