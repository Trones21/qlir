# column_bundles

`column_bundles` contains **high-level, structural DataFrame transformations** that
add *multiple, related columns* in a single, coherent operation.

These functions sit above primitive column ops and indicators. They encode
**structural context** such as grouping, segmentation, run-lengths, and
within-structure coordinates that are required for higher-order analysis
(e.g. MAE/MFE studies, persistence, regime analysis).

---

## What is a Column Bundle?

A *column bundle* is a function that:

- Operates on a `pd.DataFrame`
- Performs **multiple vectorized transformations**
- Introduces **structural semantics** (groups, legs, runs, events)
- Emits **many new columns at once**
- Returns an `AnnotatedDF` (or equivalent) with declared metadata

Column bundles are **not indicators**, **not features**, and **not signals**.

They are **structural derivations** that enrich the DataFrame so downstream
analysis can reason about *time within structure*, not just time in sequence.

---

## Typical Responsibilities

Column bundles commonly:

- Assign **group / leg identifiers**
- Add **intra-group indices** (e.g. bar position within a leg)
- Compute **per-group statistics** and broadcast them back to rows
- Mark **event rows** (e.g. max excursion within a leg)
- Add **relative position metrics** (from start / from end / percent of leg)
- Normalize or prepare columns for later summarization or bucketization

---

## Examples

### Excursion / MAE / MFE

```python
excursion(
    df,
    trendname_or_col_prefix="open_sma_14",
    leg_id_col="open_sma_14_up_leg_id",
    direction=Direction.UP,
    mae_or_mfe=ExcursionType.MFE,
)
````

Adds a coordinated set of columns describing **where and how large** the
maximum excursion occurred *within each directional leg*.

---

### Persistence / Run Lengths

```python
persistence_up_legs(
    df,
    direction_col="dir",
    trendline_col="open_sma_14",
)
```

Adds group ids, running counters, and per-leg persistence lengths for
directional trend analysis.

---

## Design Principles

Column bundles should:

* Be **fully vectorized** (no Python loops)
* Be **deterministic** and idempotent
* Avoid aggregation or data loss
* Declare all new columns via `ColRegistry`
* Log column lifecycle events for traceability

Column bundles should **not**:

* Encode trade intent
* Aggregate across groups into summaries
* Perform ML-specific scaling or encoding

---

## Relationship to Other Layers

Conceptually, `column_bundles` sit between low-level column ops and higher-level
features:

```
Scalars / Ops
→ Relations / Counters
→ Indicators
→ column_bundles      ← (this module)
→ Features
→ Signals
```

They exist to make **structure explicit**.

---

## Philosophy

If an operation answers:

> “Where am I *within* something?”

…it likely belongs in `column_bundles`.

