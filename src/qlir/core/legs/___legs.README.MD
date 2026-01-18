# core.legs

**Legs** are a first-class domain primitive in QLIR.

A **leg** represents a **contiguous, ordered run of rows** that share a **stable state**
(e.g. direction, regime, condition) over time.

Legs are *not* generic groups.

---

## What a Leg Is

A leg is defined by the following invariants:

1. **Contiguous in time**
   - Rows belonging to the same leg appear as a single continuous run
   - No interleaving with other legs
2. **Ordered**
   - Leg semantics assume a strict first → last ordering
3. **State-consistent**
   - Some state (e.g. direction, condition, trendline side) is constant within the leg
4. **Bounded**
   - Every leg has a start and an end (length ≥ 1)

If these invariants do not hold, the structure is **not a leg**.

---

## What Legs Are *Not*

Legs are **not**:

- arbitrary `groupby` groups
- unordered categories
- row-local comparisons
- generic relations between columns

Many operations that work on groups will produce **nonsense** when applied to legs
unless contiguity and ordering are respected.

---

## Scope of `core.legs`

This module contains **leg-specific primitives only**.

Included here:

- intra-leg indexing (0-based)
- leg length / bounds
- leg integrity checks (contiguity, monotonic time)
- leg-aware extrema (ordered, tie-aware)
- utilities that *require* ordered, contiguous runs

Excluded from this module:

- scalar math (belongs in `core.ops`)
- row-local predicates (belongs in `core.comparators`)
- indicators or trading logic
- generic grouping utilities


## Relationship to `df.condition_set`

Legs are **constructed** from condition sets.

The typical lifecycle is:

1. **Condition construction** (`df.condition_set.construction`)
2. **Leg ID assignment** (contiguous True-runs `df.condition_set.assign_group_ids`)
3. **Leg operations** (`core.legs`)

### Condition Sets (Upstream)

`df.condition_set` is responsible for **row-wise logical construction** of boolean
artifacts, such as:

- `all_of`, `any_of`, `at_least_k_of`
- negation and boolean composition
- explicit validation of boolean dtype and NA handling

Condition sets:
- are purely row-local
- perform no temporal reasoning
- do not define contiguity
- do not create legs by themselves

They answer the question:

> *“Is this condition true on this row?”*

---

### Leg Construction (Boundary Step)

Legs are created when a **condition set is interpreted temporally**.

This is typically done via contiguous True-run detection, e.g.:

- `df.condition_set.assign_group_ids.assign_condition_group_id`

which assigns monotonically increasing IDs to **contiguous runs** of a condition.

**Legs now exist**

---

### Leg Operations

Once a leg identifier column exists, `core.legs` operates on it.

`core.legs` assumes:
- leg IDs already exist
- rows are ordered in time
- legs are contiguous by construction

`core.legs` provides:
- intra-leg indexing
- leg length / bounds
- leg integrity checks
- leg-aware extrema and position metrics

It does **not**:
- construct condition logic
- decide what defines a leg
- perform row-wise boolean composition 
  - Note: See `column_bundles`. Many bundles use `core.legs` funcs as part of larger pipelines  

---

## Design Rule (Hard Boundary)

> `df.condition_set` **creates** legs.  
> `core.legs` **operates on** legs.

If a function decides *where a leg starts or ends*, it belongs **upstream**  
(in condition logic or group ID assignment).

If a function assumes a leg already exists and reasons *within it*, it belongs
in `core.legs`.

---

## Relationship to Other Core Modules

- `core.ops`  
  Pure scalar / vector operations. No grouping, no order assumptions.

- `core.comparators`  
  Row-local boolean predicates. No cross-row awareness.

- `core.relations`  
  Generic row↔row relationships that do *not* assume contiguity.

- `core.legs`  
  Ordered, contiguous, stateful runs over time.

---

## Downstream Usage

Legs underpin many higher-level analyses, including:

- excursions (MFE / MAE)
- stretch / position within move
- persistence and survival metrics
- trendline-relative behavior
- directional studies

These live in `column_bundles` and *consume* leg primitives rather than re-implementing them.

---

## Philosophy

Legs are introduced when a concept becomes **semantically inevitable**, not when it is convenient.

This module exists to make those assumptions explicit, enforceable, and reusable.
