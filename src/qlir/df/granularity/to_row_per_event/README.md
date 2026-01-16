# `to_row_per_event`

## Purpose

`to_row_per_event` performs a **granularity reduction**:

> **Row-aligned data → one row per event**

Each output row represents a single logical event (identified by an `event_id` column), produced by aggregating the rows that belong to that event.

This module is **purely aggregative**.
It does **not** derive columns, evaluate logic, or modify event membership.

---

## Core Principles

### 1. Event Membership Is Absolute

> **`event_id` is the sole source of truth for event membership.**

All rows with the same `event_id` belong to the same event.
No filtering or masking is performed at this stage.

---

### 2. No Column Creation

> **This module never creates new columns.**

All columns referenced during aggregation **must already exist** on the input DataFrame.

If a metric depends on:

* thresholds (`mae > 3`)
* comparisons (`A < B`)
* compound logic
* concatenated identifiers

those **must be computed beforehand** in the row-aligned phase.

---

### 3. Explicit, Single-Source Metrics

> **Each output column is derived from exactly one input column using exactly one aggregation operation.**

This allows:

* multiple aggregations over the same column (`min`, `max`, `first`, `last`)
* clear SQL-style `GROUP BY event_id` semantics
* unambiguous provenance for every output column

This disallows:

* multi-column aggregation
* expression-based metrics
* structured or multi-valued outputs

If a metric depends on multiple columns, derive it upstream as a single column first.

---

### 4. One Row per Event

> **Exactly one output row is emitted per event.**

All row-aligned data belonging to an event is collapsed into a single event-level representation.

---

## What This Module Does

* Groups rows by a stable `event_id`
* Aggregates existing columns using constrained reducers
* Emits **exactly one row per event**

Typical uses:

* Counting event-level occurrences
* Computing min/max values observed during an event
* Selecting representative values (e.g. first or last)

---

## What This Module Does *Not* Do

* ❌ Derive or mutate columns
* ❌ Filter rows or apply conditions
* ❌ Evaluate expressions or lambdas
* ❌ Perform joins or window operations
* ❌ Perform time-based chunking (see `to_row_per_time_chunk`)

---

## Metric Specification (`MetricSpec`)

Event-level outputs are defined using `MetricSpec`.

Each `MetricSpec` describes **one output column**, derived by aggregating **one existing input column** across all rows belonging to an event.

### Conceptual Definition

```python
MetricSpec(
    col: str,
    agg: Aggregation,
    *,
    out: str | None = None,
)
```

---

### Fields

#### `col` (required)

Name of the **existing input column** to aggregate.

* Must exist on the input DataFrame
* Must be row-aligned prior to aggregation

---

#### `agg` (required)

Aggregation operation applied within each event.

Supported aggregations are intentionally constrained, for example:

* `count_true`
* `min`
* `max`
* `first`
* `last`
* `sum`

---

#### `out` (optional)

Name of the output column.

* Defaults to a deterministic name derived from `(col, agg)`
* Must be unique across all metrics
* Represents exactly one semantic value

---

## Example

To associate each event with:

* its first timestamp
* its last timestamp
* the maximum MAE observed during the event
* the number of bars where MAE exceeds a threshold

### Step 1: Derive required columns (row-aligned phase)

```python
mae_gt_3 = mae > 3
```

### Step 2: Aggregate to one row per event

```python
metrics = [
    MetricSpec(col="timestamp", agg="min", out="event_start_ts"),
    MetricSpec(col="timestamp", agg="max", out="event_end_ts"),
    MetricSpec(col="sma_mae", agg="max", out="max_mae"),
    MetricSpec(col="mae_gt_3", agg="count_true", out="mae_count_gt_3"),
]
```

This means:

* All rows with the same `event_id` define the event
* Aggregation operates only on pre-existing columns
* Event aggregation remains a pure granularity reduction

This is conceptually equivalent to a SQL-style:

```
GROUP BY event_id
```

where you explicitly choose which columns survive and how they collapse.

---

## Expected Workflow

1. **Row-aligned phase**

   * Compute all indicators, conditions, and derived columns
   * Combine or concatenate columns if needed
   * Inspect and validate intermediate signals

2. **Event aggregation phase (`to_row_per_event`)**

   * Reduce rows to one row per event
   * Explicitly define event-level metrics using `MetricSpec`

3. **Downstream analysis**

   * Event-level statistics
   * Distributions and survival analysis
   * Cross-event comparisons

---

## Design Rationale

`to_row_per_event` is intentionally strict.

By forbidding column creation and conditional logic at this stage, the module:

* keeps granularity transitions explicit
* preserves column provenance
* prevents hidden feature engineering
* avoids ambiguous event representations
* remains easy to reason about and debug

This is a **lossy but honest** transformation.


Perfect — this is an important clarification, and it fits *cleanly* into the README without complicating the API.

Below is the **final addition**, written so it can be dropped straight into the README. I’ll give you:

1. The **example function signature**
2. A **new section explaining grouping columns**
3. The **continuity invariant**, stated clearly but without over-policing

---


## Example Function Signature

```python
to_row_per_event(
    df,
    *,
    event_id_col: str,
    metrics: list[MetricSpec],
    include_src_row_count: bool = False,
)

```

* `event_id_col` is the name of the column that defines **event membership**
* The column does **not** need to be named `"event_id"`
* Exactly **one grouping column** is used per call

---

## Event Grouping Semantics

### Event Identifier Column

> **Any column may be used as the event identifier.**

The `event_id_col` parameter specifies which column defines event membership for this aggregation.

Examples:

* `"event_id"`
* `"leg_id"`
* `"trend_run_id"`
* `"condition_group_id"`
* `"regime_id"`

The source DataFrame may contain **many different grouping columns** (e.g. 10–30 different event definitions).
`to_row_per_event` simply selects **one** of them for the current aggregation.

This allows the same data to be summarized along different conceptual axes without duplication.

---

### Single Grouping Column per Call

> **Each call to `to_row_per_event` groups by exactly one column.**

This is intentional.

* It keeps event semantics unambiguous
* It avoids compound grouping logic
* It ensures one row per conceptual event

If you need compound grouping:

* derive a combined grouping column upstream
* then aggregate on that column

---

### Event Continuity (Important Invariant)

> **Event identifiers are expected to represent contiguous runs in time.**

An event should correspond to a **continuous sequence of rows** when ordered by time.

This module **does not enforce continuity**, but its semantics assume it.

If an event identifier:

* reappears later after a gap
* represents disjoint time segments
* is reused across unrelated periods

the aggregation will still run, but the result may be **semantically misleading**.

If you intentionally violate continuity, you do so **at your own risk**.

---

### Why Continuity Matters

Most event-level questions implicitly assume:

* a beginning
* a duration
* an end

Examples:

* “How long did this leg last?”
* “What was the max MAE during this event?”
* “How many bars occurred in this condition?”

These questions only make sense when the event forms a **single contiguous span of time**.

---

### Summary

* The source DataFrame may contain **many grouping columns**
* Each `to_row_per_event` call selects **one**
* Event IDs should represent **contiguous time spans**
* No enforcement is performed — correctness is the caller’s responsibility


### Example Use of to_row_per_event

```python
metrics = [
    MetricSpec(col="timestamp", agg=Aggregation.MIN, out="event_start_ts"),
    MetricSpec(col="timestamp", agg=Aggregation.MAX, out="event_end_ts"),
    MetricSpec(col="mae_gt_3", agg=Aggregation.COUNT_TRUE),
    MetricSpec(col="sma_mae", agg=Aggregation.MEDIAN),
]

df_event = to_row_per_event(
    df,
    event_id_col="leg_id",
    metrics=metrics,
    include_src_row_count=True,
)
```

---

## Aggregation → dtype compatibility (authoritative rules)

### Allowed dtypes per aggregation

| Aggregation     | Allowed dtypes               | Rationale         |
| --------------- | ---------------------------- | ----------------- |
| `MIN`, `MAX`    | numeric, datetime, timedelta | Ordering required |
| `FIRST`, `LAST` | any                          | Positional        |
| `SUM`           | numeric, boolean             | Additive          |
| `MEDIAN`        | numeric                      | Ordered numeric   |
| `COUNT_TRUE`    | boolean only                 | Logical count     |

Notes:

* `boolean` is allowed for `SUM` but **not** encouraged (still valid).
* `MEDIAN(datetime)` is intentionally **not allowed**.
* `COUNT_TRUE` must be **strictly boolean**, not truthy.



