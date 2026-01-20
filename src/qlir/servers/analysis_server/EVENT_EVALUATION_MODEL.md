# Event Evaluation & Composition Model

This document defines **how triggers are evaluated within a single analysis loop iteration**, and how **pipeline**, **event-based**, and **non-event-based** triggers coexist.

---

## Core invariant

> **All trigger evaluation is scoped to a single analysis loop iteration
> (i.e. one new `data_ts`).**

No trigger state is carried across iterations unless explicitly encoded in DataFrames.

---

## Terminology

* **Pipeline trigger**
  A trigger that validates **system and data integrity** (e.g. stale data, missing slices).
  Pipeline triggers are evaluated **before any market logic**.

* **Event-based trigger**
  A trigger whose truth represents an **atomic market fact** for the current bar.
  Event-based triggers populate the **event fact set**.

* **Non-event-based trigger**
  A trigger that:

  * does *not* populate the event fact set
  * may be:

    * DF-backed, or
    * composed from event-based triggers

  These include tradable and positioning triggers.

* **Analysis loop iteration**
  One execution of the analysis loop after a new watermark (`data_ts`) is observed.

---

## Execution model (per iteration)

Each analysis loop iteration proceeds in **strict, ordered phases**.

---

### Phase 0 — Initialization

```python
triggered_events: set[str] = set()
```

This set is **empty at the start of every iteration**.

It represents **event facts that are true for this bar only**.

---

### Phase 1 — Pipeline trigger evaluation (must run first)

* Pipeline triggers are evaluated **before any market logic**
* They validate:

  * data freshness
  * data completeness
  * system correctness
* Pipeline triggers may emit alerts immediately

Pipeline triggers:

* do **not** populate `triggered_events`
* do **not** depend on any other trigger class
* may short-circuit *human trust*, but **do not stop execution** by default

> Pipeline alerts answer:
> **“Can the rest of this iteration be trusted?”**

---

### Phase 2 — Event-based trigger evaluation

* Only **event outboxes** are evaluated
* Each event-based trigger is evaluated against the latest DF row
* If an event-based trigger evaluates to `True`:

  * its trigger key is added to `triggered_events`
  * the event alert is emitted

Example state:

```text
triggered_events = {
  "sma_14_up_started",
  "bb_midline_touch"
}
```

This set answers one question only:

> *“Which atomic market facts are true right now?”*

---

### Phase 3 — Non-event-based trigger evaluation

Non-event-based triggers (tradable / positioning) are evaluated **after** events.

They fall into two categories:

#### 1. DF-backed, non-event-based triggers

* Evaluated directly from their `(df, column)` pair
* Do **not** add entries to `triggered_events`

#### 2. Event-composed, non-event-based triggers

* Evaluated **only** by querying `triggered_events`
* Never read DF columns from other triggers

Supported logic (initially):

* `ALL`
  All required event keys must exist in `triggered_events`

* `ANY`
  At least one required event key must exist

* `N_OF_M`
  (defined later)

Example:

```python
required = {"sma_14_up_started", "bb_midline_touch"}

if required.issubset(triggered_events):
    fire_tradable()
```

---

### Phase 4 — End of iteration

* Alerts have been emitted
* `triggered_events` is discarded
* No trigger state survives the loop boundary

---

## Key invariants (non-negotiable)

* **Pipeline triggers always run first**
* **`triggered_events` is reset every analysis loop iteration**
* **Only event-based triggers populate `triggered_events`**
* **Non-event-based triggers may only read from `triggered_events`**
* **No trigger evaluation depends on past iterations**
* **Temporal persistence must be encoded in DataFrames, not trigger state**

---

## Why this model exists

This design guarantees:

* System trust is established before market interpretation
* Deterministic behavior (same data → same alerts)
* Clear evaluation order
* No hidden temporal coupling
* Easy replay and debugging
* Simple reasoning under live conditions

It also keeps the analysis server:

* non-stateful
* non-recursive
* mechanically simple

---

## Mental model (final)

> **Each analysis loop iteration proceeds as:**
>
> 1. *Can I trust the data?* (pipeline)
> 2. *What facts are true right now?* (events)
> 3. *What do those facts imply?* (non-event-based)
>
> All within a single, disposable execution frame.

Nothing more.

---

### Naming note (intentional)

The system avoids terms like:

* “derived”
* “secondary”
* “higher-order”

because they obscure the only runtime distinction that matters:

> **Does this trigger validate trust, emit facts, or consume facts?**

This document defines that boundary explicitly.
