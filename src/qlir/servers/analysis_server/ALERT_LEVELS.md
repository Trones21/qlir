## 4️⃣ Alert levels (ownership-based taxonomy)

Alerts are classified by **who should act**, not by market theory.

This avoids overloaded terminology and keeps the system socially and operationally correct.

---

### 0️⃣ Events — *Structural Telemetry*

**Who should act**

* 🤖 Bots: *possibly* (if promoted or aggregated)
* 🧑 Humans: *very unlikely*

**Purpose**

* ground truth of market state
* lossless structural facts
* debugging, aggregation, replay

**Characteristics**

* high frequency
* zero implied intent
* never actionable alone

**Bot**

```text
qlir-events
```

Muted. Treated like logs.

---

### 1️⃣ Tradable — *Executable Opportunity*

**Who should act**

* 🤖 Bots: **yes (any frequency)**
* 🧑 Humans: **yes (minutes+ latency)**

**Purpose**

* executable edge
* tactical opportunities
* “this market is usable *now*”

**Key distinction**

* Frequency is **signal-specific**
* Human usability depends on **persistence**, not immediacy

**Bot**

```text
qlir-tradable
```

Notifications on, not loud.

---

### 2️⃣ Positioning — *Exposure Awareness*

**Who should act**

* 🤖 Bots: **no**
* 🧑 Humans: **yes**

**Purpose**

* portfolio exposure management
* prevent being naked into expansion
* prevent missing structurally meaningful moves

**Characteristics**

* low frequency
* large displacement potential
* bidirectional (direction irrelevant)

**Bot**

```text
qlir-positioning
```

Loud notifications.

---

### 3️⃣ Pipeline — *Operational Integrity*

**Who should act**

* 🤖 Bots: no
* 🧑 Humans: **yes (immediately)**

**Purpose**

* system trust
* signal validity
* operational correctness

**Bot**

```text
qlir-pipeline
```

Always on.

---

## 5️⃣ Ownership matrix (canonical)

| Alert Level | Bots should trade | Humans should trade | Human latency tolerance |
| ----------- | ----------------- | ------------------- | ----------------------- |
| Events      | ❌ (rare)          | ❌                   | N/A                     |
| Tradable    | ✅ (any freq)      | ✅ (minutes+)        | Medium                  |
| Positioning | ❌                 | ✅                   | High                    |
| Pipeline    | ❌                 | ❌                   | Immediate               |

If an alert does not fit **exactly one row**, it is misclassified.

---

## 6️⃣ Events as an external interface

Although higher-level alerts may aggregate or interpret events, **events remain first-class** for a critical reason:

> **Events are the stable, lossless interface for downstream consumers outside the QLIR ecosystem.**

This includes:

* webhooks
* dashboards
* external services
* ML pipelines
* future systems with different semantics

---

## 7️⃣ Composition of tradables (out of scope here)

QLIR intentionally does **not** prescribe how events are combined into tradable signals.

Valid approaches include:

* building tradable DataFrames directly
* composing multiple events into a tradable trigger
* delegating interpretation to external consumers

These options are documented separately.

📄 **See:** `COMPOSING_TRADABLES.md`

This separation keeps the base architecture:

* stable
* minimal
* non-opinionated
* future-proof
