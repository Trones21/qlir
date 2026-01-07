# Condition Groups, Age, Activation, and Group-Scoped Aggregations

## Purpose

This document defines the **semantic model** QLIR uses to reason about boolean conditions over time.

QLIR is a **row-preserving, column-oriented research system**.
Most primitives are thin, intentional wrappers over existing DataFrame functionality (e.g. pandas), with the goal of making *meaning explicit* rather than inventing new mechanics.

The focus here is **structure and timing**, not domain-specific interpretation.

---

## 1. Condition Columns

A **condition** is any boolean-valued column defined over the full DataFrame.

Examples:

* `close_above_bb_mid`
* `atr_below_1_1`
* `volatility_regime_high`

Properties:

* evaluated independently per row
* stateless
* globally defined (even if not always relevant)


`<to do add table example>`


### Why: Conditions are stateless and global

This prevents a **ton** of conceptual bugs.

A condition:

* does not “start”
* does not “end”
* does not “know” about neighbors

It is *purely row-local*.

That’s exactly why persistence must be downstream.

---

## 2. Row-wise Logical Composition

Conditions can be combined row-wise into higher-level boolean artifacts.

### ALL (logical conjunction)

True only if **all conditions are true** on that row.

```
ALL([a, b, c])
```

Equivalent to:

```
a AND b AND c
```

---

### ANY (logical disjunction)

True if **at least one condition is true**.

```
ANY([c, d])
```

Equivalent to:

```
c OR d
```

---

### K-of-N / Subset

True if **K out of N conditions are true**.

```
AT_LEAST_K([a, b, c], k=2)
```

These are **first-class semantic concepts**, not syntactic sugar.

---

## 3. Materialized Logic as Artifacts (Workflow Rule)

Logical compositions should be **persisted as named boolean columns**, not left implicit.

Examples:

* `all(a,b,c)`
* `any(c,d)`
* `k2_of(a,b,c)`

`<to do add table example>`

These columns are the **next artifacts** in the pipeline and enable composability.

QLIR workflows are intentionally **left-to-right**:

> primitive signals → conditions → logical compositions → row runs → gating → aggregation

Persisting each step:

* makes intent explicit
* avoids re-expressing logic inline
* enables reuse across multiple purposes

Different logical compositions may exist simultaneously for different intents.

Examples:

* `enter_all(a,b,c)`
* `exit_idx_gt_5_and_any(c,d)`
* `analysis_all(a,b,c)`

This asymmetry is expected.

---

## 4. Row Runs (Contiguous True Segments)

Given any **boolean column**, we can define a purely structural concept based on contiguity.

### Definition

A **row run** (also called a *row group* or *true-run*) is:

> A temporally contiguous set of rows where a specific boolean column is `True`.

The boolean column may represent:

* a single condition
* a logical composition (ALL / ANY / K-of-N)
* any derived signal

The run itself carries **no inherent meaning**.

---

### Example

Given a DataFrame with a boolean column:

| row_id | some_bool_column |
| -----: | :--------------- |
|      0 | False            |
|      1 | True             |
|      2 | True             |
|      3 | True             |
|      4 | False            |
|      5 | True             |
|      6 | True             |
|      7 | False            |
|      8 | True             |

This produces **three row runs**:

* **Run 1**: rows 1–3
* **Run 2**: rows 5–6
* **Run 3**: row 8

Each row run:

* starts at the first `True` after a `False`
* ends immediately before the next `False`
* is maximal (cannot be extended without including a `False`)

---

### Properties

A row run:

* is defined solely by contiguity
* does not imply interpretation
* can be assigned a `run_id`
* establishes a local frame for further analysis

Meaning is layered **on top of** row runs, not embedded within them.

---

## 5. Condition Age (Run-Relative Index)

### Definition

**Condition age** tracks position *within a row run*.

Semantics:

* `NaN` (or −1) when the boolean column is `False`
* `0` on the first row of a run
* increments by 1 for each subsequent row
* resets when the run ends

This creates a **run-relative coordinate system**.

`<to do add table example>`

---

### Why It Exists

Many behaviors depend on *time since a run began*, not absolute position:

* early vs late behavior
* delayed activation
* persistence analysis
* gating logic

Condition age is the **minimal primitive** needed to express these patterns.

---

## 6. Condition Activation (Gating)

### The Core Problem

Some conditions may be **true at the start of a run**, but **must not be acted upon yet**.

Example:

* a boolean condition is satisfied on the first row
* another condition should only matter *after* some delay

This is an **activation problem**, not a logical contradiction.


`<to do add table example>`

---

### Activation as Data

Conditions are **defined globally** but may only be **active locally**.

Activation is expressed declaratively via additional boolean logic.

Example:

```
active_after_start = (condition_age >= 1)
gated_condition    = active_after_start AND some_other_condition
```

`<to do add table example>`

This avoids:

* procedural state
* special casing
* ambiguous timing behavior

QLIR does not prescribe *how* gated conditions are used — only how they are expressed.

---

## 7. Group-Scoped Aggregations

### Motivation

Some properties are about **behavior within a row run**, not individual rows.

Examples:

* count of times a specific column is of a specific value within a run


`<to do add table example>`

These are **aggregations**, not conditions.

You could make another column that represents a condition that the aggregation may or may not adhere to:
* whether this occurred at least N times
* whether it occurred by a certain run age
---

### Semantics

Group-scoped aggregations:

* operate within a `run_id`
* do not require contiguity of the inner condition
* reset only when the run resets
* may be cumulative or final

They are typically thin wrappers over DataFrame primitives such as:

* `groupby().transform(...)`
* cumulative sums
* expanding windows within runs

---

## 8. Two Valid Representations of Run-Level Facts

Once a `run_id` exists, run-level facts may be represented in two equivalent ways.

### Row-Embedded (Denormalized)

Run facts attached to every row.

`<to do add table example>`

Pros:

* easy to combine with condition age
* useful for row-relative gating
* no joins required

Cons:

* semantically redundant

---

### Run-Summarized (Normalized)

One row per run, then joined back if needed.

`<to do add table example>`

Pattern:

1. summarize per run
2. filter runs
3. join back on `run_id`

Pros:

* conceptually clean
* ideal for run-level filtering

Cons:

* requires explicit join

---

### Design Rule

> Row-level and run-level representations are duals.
> Choose based on whether logic is **row-relative** or **run-global**.

QLIR does not force one representation.

---

## 9. Recommended Naming (Minimal, Advisory)

Column names are **semantic labels**, not programming identifiers.

Suggestions:

* readability over strict `snake_case`
* express intent, not implementation
* keep names short but descriptive

Examples:

* `all(a,b)`
* `any(c,d)`
* `all(a,b) | any(c,d)`
* `exit_idx_gt_5_and_any(c,d)`

Symbols and parentheses are allowed if they improve clarity.
Names are labels, not executable logic.

For math symbols, check out [Setting Up a Compose Key for Math & Greek Symbols](./setup_compose.md
---

## 10. Key Principles

* Conditions are row-level booleans
* Logical compositions are materialized as columns
* Row runs are defined by contiguous `True` values
* Condition age is run-relative
* Activation is data, not control flow
* Run-level facts may be denormalized or summarized
* Row cardinality is preserved unless explicitly collapsed

---

## 11. Scope and Status

This document defines **foundational semantics**.

* APIs may evolve
* implementations may change
* these concepts are intended to remain stable

They form the conceptual substrate for studies and higher-level logic in QLIR.

