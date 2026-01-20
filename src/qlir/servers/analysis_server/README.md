# QLIR Analysis Server — Base Architecture & Alert Model

This document defines the **core invariants**, **signal materialization rules**, **alert ownership taxonomy**, and **routing model** for the QLIR analysis server.

It intentionally avoids prescribing *how* signals are composed or *how* notifications are delivered internally.
Those concerns are explicitly separated.

---

## 1️⃣ Core invariant

> **The analysis server evaluates facts, not strategies.**

It:

* does not reason
* does not branch on intent
* does not combine signals
* does not know portfolio state
* does not know strategy
* does not know delivery targets

All semantics live **upstream** (DataFrame construction) or **downstream** (alert routing & notification).

---

## 2️⃣ Analysis server control flow (invariant)

Every loop iteration:

```
1. Load latest parquet window
2. If empty → sleep
3. Extract last row
4. Extract data_ts
5. Compare with last_processed_data_ts
   └─ if not newer → sleep
6. Check data freshness
   └─ if stale → emit pipeline alert
7. Evaluate trigger DataFrames
   └─ emit alerts for any triggered facts
8. Persist last_processed_data_ts
9. Sleep
```

There is **no conditional branching beyond this**.

If complexity appears here, the architecture has failed.

---

## 3️⃣ Signal materialization model

### Core rule

> **One semantic “thing” → one final DataFrame**

Not:

* one DataFrame with many trigger columns
* one DataFrame per alert level
* one DataFrame per timeframe

But:

* **one DataFrame per hypothesis / trigger construct**

This is intentional, even if it increases memory usage.

---

### Why this rule exists

A DataFrame is not just storage — it is a **frozen mental model**:

* one intent
* one horizon
* one debounce expectation
* one alert level
* one downstream audience

This ensures:

* clarity during live operation
* clean attribution during analysis
* safe evolution of signals
* deterministic alert replay

---

### Examples

```text
df_event_sma_direction_flip
df_event_leg_boundary

df_tradable_spot_led_vol_expansion
df_tradable_liq_event

df_positioning_multi_day_vol_expansion
df_positioning_path_length_accel

df_pipeline_data_stale
df_pipeline_partial_slice
```

It is expected and correct to have:

* multiple DataFrames within the same alert level
* multiple variants of the “same idea” (fast / slow, spot / perp, etc.)

These represent **distinct hypotheses**, not configuration noise.

---

### Trigger evaluation rule

Each trigger DataFrame must satisfy:

* exactly one boolean trigger column (or equivalent)
* the final row fully determines alert emission
* no dependency on other trigger DataFrames at evaluation time

The analysis server simply does:

```
for each trigger_df:
    if trigger_df.last_row.triggered:
        emit alert
```
---

## 4️⃣ Alert levels (ownership-based taxonomy)

Alerts are classified by **who should act**, not by market theory.

| Alert Level | Bots should trade | Humans should trade | Human latency tolerance |
| ----------- | ----------------- | ------------------- | ----------------------- |
| Events      | ❌ (rare)          | ❌                   | N/A                     |
| Tradable    | ✅ (any freq)      | ✅ (minutes+)        | Medium                  |
| Positioning | ❌                 | ✅                   | High                    |
| Pipeline    | ❌                 | ❌                   | Immediate               |

If an alert does not fit **exactly one row**, it is misclassified.

See ALERT_LEVELS.md for more depth on alert levels.

---

## 5️⃣ Events as an external interface

> **Events are the stable, lossless interface for downstream consumers outside the QLIR ecosystem.**

* Events → facts (exportable, replayable, strategy-agnostic)
* Tradable / Positioning → interpretations (QLIR-native, opinionated)

> **Events are an API.
> Alerts are a UI.**

---

## 6️⃣ Wiring & Outboxes (new invariant)

### Motivation

Alert *meaning* and alert *delivery* are intentionally decoupled.

To support:

* multiple bots
* multiple humans
* multiple venues
* multiple notification servers
* priority handling
* rate limiting
* horizontal scaling

QLIR introduces a **separate wiring layer**.

---

### Core rule

> **Alert generation does not determine delivery.
> Delivery is resolved via an explicit wiring registry.**

---

### Wiring registry

A **wiring registry** maps:

```
(trigger or alert class) → outbox
```

This registry is:

* declarative
* external to the analysis server
* shared with the notification server(s)

Example (conceptual):

```yaml
wiring:
  events:
    outbox: qlir-events

  tradable:
    binance_bot: qlir-tradable-binance-bot
    drift_bot:   qlir-tradable-drift-bot
    human:       qlir-tradable-human

  positioning:
    outbox: qlir-positioning

  pipeline:
    outbox: qlir-pipeline
```

---

### Outboxes

An **outbox** is a named, durable routing target.

Examples:

```text
qlir-events
qlir-tradable-binance-bot
qlir-tradable-drift-bot
qlir-tradable-human
qlir-positioning
qlir-pipeline
```

Outboxes are:

* independent
* fan-out capable
* assignable to different notification servers

---

### Notification servers

Notification servers:

* subscribe to **one or more outboxes**
* do **no signal logic**
* apply delivery policies only

This allows:

* multiple notification server instances
* per-outbox rate limits
* per-outbox priority
* per-outbox transport (Telegram, webhook, etc.)

Example deployments:

* **High-priority server**

  * `qlir-positioning`
  * `qlir-pipeline`

* **Bot execution server**

  * `qlir-tradable-binance-bot`
  * `qlir-tradable-drift-bot`

* **Human notification server**

  * `qlir-tradable-human`
  * `qlir-positioning`

---

### Why this is an invariant (not an implementation detail)

This separation guarantees:

* alert semantics remain stable
* delivery can evolve independently
* humans and bots never compete for the same channel
* operational scaling does not leak into signal logic
* priority is enforceable by config, not code

---

## 7️⃣ Composition of tradables (out of scope here)

How `events` are combined into `tradable` signals is **explicitly user-defined**.

📄 **See:** `COMPOSING_TRADABLES.md`

This document covers:

* event composition
* ALL / ANY / N-of-M logic
* DF-level vs post-DF composition
* notification throttling strategies

---

## 8️⃣ Mental model (final)

> The analysis server:
>
> **evaluates facts, emits alerts,
> and leaves routing, delivery, and priority
> to a separate wiring layer.**

Nothing more.

Everything else is plumbing.

---

## 9️⃣ Locked invariants (summary)

* ✔ Analysis loop is mechanically trivial
* ✔ One DataFrame per semantic trigger
* ✔ Alert meaning is defined by action ownership
* ✔ Events are first-class and exportable
* ✔ Tradable supports bot-only and human-usable signals
* ✔ **Delivery is resolved via a wiring registry**
* ✔ Outboxes are first-class routing targets
* ✔ Notification servers are stateless consumers

---

### Final note

This wiring layer is what makes it possible to:

* scale safely
* introduce new bots without touching signal logic
* give humans priority without starving automation
* and keep QLIR evolvable without rewrites
