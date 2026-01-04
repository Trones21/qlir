# `comparators.py`

> Atomic, alignment-safe comparison primitives for pandas DataFrames.

This module provides **low-level, pointwise comparison helpers** that generate **boolean columns** describing relationships between values **at the same index**.

It is intentionally minimal and opinionated:

* no temporal logic
* no range semantics
* no events or strategy decisions

Those concepts are built **on top of** this layer.

---

## What this module is for

Use `comparators.py` to answer questions like:

* “Is `close` greater than `open_sma_14` at this bar?”
* “Is `abs_change` less than `0.01` at this index?”
* “Are two floating indicators approximately equal?”

Each comparison:

* is evaluated **row-wise**
* preserves **index alignment**
* produces a **boolean column**
* behaves deterministically under `NaN`

---

## What this module is *not* for

This module does **not** handle:

* temporal transforms (`shift`, lags, lookbacks)
* crossovers or transitions
* range semantics (inside / outside bands)
* regime or event detection
* procedural logic (`if`, branching, signals)

If a comparison requires time awareness, that time context **must be made explicit before comparison**.

---

## Core design principles

### 1. Alignment-first semantics

All comparisons operate on aligned Series.

```python
df["a"] > df["b"]          # valid
df["a"] > df["b"].shift(1) # also valid (alignment preserved)
```

No slicing, no index arithmetic, no off-by-one errors.

---

### 2. Explicit temporal transforms (required)

Temporal logic **must not** be embedded inside comparators.

❌ **Not supported (will fail):**

```python
with_gt(df, "a", shift("a", 3))
```

Comparators accept **only**:

* column names (`str`)
* scalars (`int | float`)

Passing shifted Series or symbolic temporal expressions is **not supported and will fail**.

✅ **Correct usage:**

```python
df["a_lag_3"] = df["a"].shift(3)
with_gt(df, "a", "a_lag_3")
```

---

### 3. Atomic predicates only

Each comparator answers **one question per row**:

> “Does relation *R* hold between *A* and *B* at this index?”

No memory.
No history.
No hidden state.

---

### 4. Deterministic boolean output

All results:

* use pandas `"boolean"` dtype
* replace `NaN` with `False`
* behave predictably under missing data

This guarantees safe downstream composition.

---

### 5. Tolerance-aware numeric equality

Floating-point equality supports tolerance:

```python
with_eq(df, "x", "y", tol=1e-6)
```

Defined as:

```
|x − y| ≤ tol
```

This avoids false negatives from floating-point noise.

---

## Public API

Each function returns a DataFrame with a new boolean column.

| Function             | Meaning |       |       |
| -------------------- | ------- | ----- | ----- |
| `with_gt(a, b)`      | a > b   |       |       |
| `with_ge(a, b)`      | a ≥ b   |       |       |
| `with_lt(a, b)`      | a < b   |       |       |
| `with_le(a, b)`      | a ≤ b   |       |       |
| `with_eq(a, b, tol)` |         | a − b | ≤ tol |
| `with_ne(a, b, tol)` |         | a − b | > tol |

Arguments `a` and `b` may be:

* column names
* scalars (broadcast to index)

---

## Naming behavior

If `name` is not provided, column names are auto-generated:

```python
with_gt(df, "close", "open_sma_14")
```

Produces:

```text
close__gt__open_sma_14
```

Names are intentionally verbose to preserve semantic meaning through:

* pipelines
* debugging
* serialization
* downstream condition sets

---

## Example usage

### Simple comparison

```python
df = with_gt(df, "close", "open_sma_14")
```

### Scalar threshold

```python
df = with_lt(df, "abs_change", 0.01)
```

### Temporal self-comparison (explicit)

```python
df["sma_lag_5"] = df["open_sma_14"].shift(5)
df = with_gt(df, "open_sma_14", "sma_lag_5")
```

---

## Composition (intended usage)

Comparator outputs are meant to be **combined**, not acted on directly.

```python
quiet_uptrend = (
    df["close__gt__open_sma_14"]
    & df["abs_change__lt__0.01"]
)

df["regime_quiet_up"] = quiet_uptrend
```

This enables:

* regime labeling
* condition grouping
* higher-level signal logic

---

## Relationship to other modules

* **`comparators.py`** → atomic pointwise predicates
* **`range_relations.py`** → point ↔ interval semantics (built on comparators)
* **`crossovers.py`** → temporal transitions (built on shifted comparisons)
* **`condition_set`** → grouping and reduction of predicate columns

This file is the **bottom layer** of the predicate stack.

---

## In one sentence

> **If you need to ask “is A related to B at this index?”, use `comparators.py`.
> If the question involves time, ranges, or events, build that first — then compare.**

---

If you want next, we can:

* mirror this with a `README` for `range_relations.py`
* add a short **module-level docstring** version
* or formalize a “predicate stack” diagram for the core docs
