
# Alert Outboxes

### 1️⃣ Analysis server = **outbox declarer**

The analysis server is allowed to say:

> “These outboxes exist for this run / deployment.”

But it must say this in a way that is:

* durable
* restart-safe
* cross-process
* non-imperative

### ✅ The correct mechanism: **a written declaration file**

For example:

```text
$QLIR_STATE_DIR/outboxes.json
```

or

```text
$QLIR_STATE_DIR/outboxes.yaml
```

---

### Example: `outboxes.json`

```json
{
  "version": 1,
  "generated_at": "2026-01-20T11:42:00Z",
  "outboxes": {
    "qlir-events": {
      "alert_level": "events",
      "priority": "low"
    },
    "qlir-tradable-binance-bot": {
      "alert_level": "tradable",
      "priority": "medium"
    },
    "qlir-tradable-drift-bot": {
      "alert_level": "tradable",
      "priority": "medium"
    },
    "qlir-tradable-human": {
      "alert_level": "tradable",
      "priority": "medium"
    },
    "qlir-positioning": {
      "alert_level": "positioning",
      "priority": "high"
    },
    "qlir-pipeline": {
      "alert_level": "pipeline",
      "priority": "critical"
    }
  }
}
```

This file is:

* **authoritative**
* **append-only or overwrite-on-start**
* written once at startup (or on change)
* safe to read at any time

---

### 2️⃣ Notification servers = **outbox discoverers + subscribers**

Notification servers:

* read `outboxes.json`
* validate that requested outboxes exist
* subscribe only to what they are configured for

They may use **env vars** — for *selection*:

```bash
QLIR_OUTBOXES=qlir-positioning,qlir-pipeline
```

or

```bash
QLIR_OUTBOXES=qlir-tradable-binance-bot
```

Env vars answer:

> “Which outboxes do *I* handle?”

The file answers:

> “Which outboxes exist?”

---

## Why this is better than a “coordinator” (right now)

A **file is the simplest coordinator**:

* no network
* no liveness issues
* no race conditions
* no boot ordering problems
* trivial to debug
* works on one machine or many (shared volume)

We can always promote it later to:

* SQLite
* Redis
* S3
* Kafka
* etc.

The contract stays the same.

---

## What the analysis server is *not* allowed to do

Let’s be explicit — to avoid future confusion.

The analysis server:

* ❌ does NOT decide which notification server runs
* ❌ does NOT set env vars
* ❌ does NOT assume any consumer exists
* ❌ does NOT retry delivery
* ❌ does NOT enforce priority

It only **declares existence**.
