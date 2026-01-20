Perfect — this is a **real architectural fork**, and you’re handling it the right way:
*don’t over-commit now, but document the escape hatches clearly.*

Below is a **clean, doc-ready section** you can append to the existing document. It states the options, the tradeoffs, and the invariants **without forcing a decision**.

---

## 9️⃣ From Events → Tradable: Composition Models (Explicitly Flexible)

There is **no single mandated workflow** for how `events` become `tradable` signals.

This is intentional.

QLIR supports **multiple equivalent composition strategies**, and the choice is left to the user.

---

## Core principle

> **Events are atomic facts.
> Tradable signals are interpretations.
> How facts are combined is an implementation choice, not an invariant.**

---

## Option A — Build tradable DataFrames directly

The simplest and most common path.

* A `tradable` DataFrame:

  * derives its own columns
  * computes its own trigger condition
  * emits alerts independently of explicit `event` consumption

This is effectively a **monolithic DF approach**, even if it reuses lower-level helpers.

**Pros**

* Maximum expressive power
* Natural place for complex logic
* Fewer DataFrames

**Cons**

* Less composable
* Harder to reuse partial facts
* Logic can become dense

---

## Option B — Composable event-based tradables (same loop, DF-level)

A `tradable` trigger may be defined as a **logical composition of multiple events**, evaluated in the same analysis loop iteration.

Conceptually:

```json
{
  "required_event_triggers": [
    "open_sma_14_up_5%_survive",
    "bb_midline_has_touched_at_least_3_times_in_last_30_bars"
  ]
}
```

This is **functionally equivalent** to:

* building all required columns into a single DataFrame
* adding a final boolean column that ANDs them together

The difference is **where the combinatorial logic lives**.

Here, the logic is pushed *past* the individual event DataFrames.

---

### Characteristics of composable triggers

* No cross-DF dependencies beyond boolean state
* Composition occurs at the **alerting layer**
* Events must be evaluated in the **same loop iteration**
* Logical forms may evolve:

  * `ALL`
  * `ANY`
  * `N of M`
  * (future)

---

### Pros

* Extremely fast to compose new signals
* Reuses existing event definitions
* Avoids rewriting or duplicating DF logic
* Encourages small, atomic event definitions

---

### Cons (explicit)

* More DataFrames overall
* Harder to express deeply stateful logic
* Requires a different mental model:

  * signals may fire **repeatedly**
  * not always as a single “edge event”

Example shift in thinking:

Instead of:

```text
open_sma_14_up_5%_survive   # fires once
```

You may need:

```text
open_sma_14_up_still_under_5%_survival   # fires every bar until exit
```

This allows composition logic to decide *when* to notify.

---

### Notification guidance (important)

If composable triggers are used:

* **They should remain active triggers**
* Their DataFrames should be written as usual
* **Notification throttling should happen downstream**

For example:

* Telegram:

  * suppress per-bar spam
  * emit one summarized message
* Webhooks:

  * free to emit every occurrence if desired

In other words:

> **Do not disable the signal — control the delivery.**

---

## Option C — External event consumer (future / optional)

A separate process *may* exist that:

* reads `events` only
* applies its own logic
* emits its own alerts or actions
* is not constrained by QLIR’s alert taxonomy

This process:

* is explicitly **out of scope** for the analysis server
* treats `events` as a stable input stream
* may evolve independently

This option exists to preserve architectural freedom — **not** as a current requirement.

---

## Summary (locked intent)

* Events → Tradable mapping is **user-defined**
* Multiple composition strategies are valid
* Composable triggers are equivalent to DF-level logic, just relocated
* Notification spam is a delivery concern, not a signal concern
* External consumers are explicitly supported via events

Nothing in this section alters:

* the invariant analysis loop
* the one-DF-per-thing rule
* the alert ownership taxonomy

It simply documents the **degrees of freedom** — so future you doesn’t have to rediscover them.

---

If you want next (optional, later):

* formalize an `EventCompositionSpec`
* define a minimal boolean-only composition engine
* or design a notification coalescer for composable triggers

But for now: this is exactly the right level of explicitness, without over-engineering.
