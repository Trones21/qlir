
1. **What OPS Watcher *is*** (conceptual contract)
2. **Service architecture** (components + data flow)
3. **README draft** (so you can drop it in immediately)

# 1. What OPS Watcher *is*

**OPS Watcher** is a **local operational signal generator**.

It:

* Periodically inspects the system
* Evaluates expectations (processes, logs, etc.)
* Emits **event JSON files** *only when expectations are violated*
* Uses the existing **notification server** as its sole downstream consumer

It is **not**:

* A metrics system
* A dashboard
* A supervisor
* A restart manager
* A Prometheus exporter

Its job is simply to answer:

> “Does reality still match what I believe should be happening?”

---

# 2. Architecture

## 2.1 High-level shape

```
┌──────────────────┐
│   OPS Watcher    │
│                  │
│  ┌────────────┐  │
│  │ Scheduler  │◄─┼─ interval (e.g. 60s)
│  └─────┬──────┘  │
│        │         │
│  ┌─────▼──────┐  │
│  │  Checkers  │  │
│  │            │  │
│  │  • process │  │
│  │  • logs    │  │
│  │  • (future)│  │
│  └─────┬──────┘  │
│        │         │
│  ┌─────▼──────┐  │
│  │  Evaluator │  │
│  └─────┬──────┘  │
│        │         │
│  ┌─────▼──────┐  │
│  │  Outbox    │──┼──► notification server
│  │  (JSON)    │  │
│  └────────────┘  │
└──────────────────┘
```

---

## 2.2 Core concepts (important)

### **Check**

A *check* is a single expectation about the system.

Examples:

* “A process with this `proc_cmdline` exists”
* “This log file has grown by ≥ X bytes in 24h”

Checks are:

* Declarative
* Stateless in isolation
* Evaluated periodically

---

### **Checker**

A checker is a **category of checks**.

Initial checkers:

* `process`
* `log_growth`

Future checkers might include:

* disk space
* heartbeat files
* network reachability
* queue backlog size

Each checker:

* Knows how to *observe*
* Knows how to *evaluate*
* Emits standardized **ops events**

---

### **Ops Event**

An ops event is a **fact**, not a metric.

Emitted only when:

* a check fails
* or transitions from OK → NOT OK (later optimization)

Written as a JSON file to the outbox.

---

## 2.3 Configuration format choice

Given your preferences and goals:

### ✅ Use **TOML**

Why TOML fits OPS Watcher:

* Human-readable
* Strict typing
* No YAML indentation footguns
* Better comments than JSON
* Already familiar in Python ecosystems

This also avoids JSON’s “no comments” pain for ops config.

---

## 2.4 Example config (OPS Watcher)

```toml
[service]
name = "qlir_ops_watcher"
interval_seconds = 60
emit_outbox = "/qlir/outbox/ops"

# OPS Watcher should verify itself is alive
[self_check]
enabled = true
proc_cmdline_contains = "ops_watcher.py"

[[process_checks]]
name = "qlir_data_server"
proc_cmdline_contains = "qlir/data_server.py"
severity = "critical"
note = "Must match actual Python entrypoint, not tmux"

[[process_checks]]
name = "qlir_analysis_server"
proc_cmdline_contains = "qlir/analysis_server.py"
severity = "critical"

[[log_growth_checks]]
name = "analysis_log"
path = "/var/log/qlir/analysis.log"
min_growth_bytes = 1024
window_hours = 24
severity = "warning"
```

This already:

* Checks itself
* Separates concerns cleanly
* Scales to new check types without refactoring

---

## 2.5 Self-check (important and correct)

OPS Watcher **must check itself**, but with nuance:

* It **cannot detect its own failure**
* It *can* detect:

  * multiple instances
  * mis-launch
  * wrong binary
  * wrong arguments

If OPS Watcher stops entirely, silence *is* the signal — and that’s OK.

This matches Unix philosophy.

---

## 2.6 State (minimal, but necessary)

OPS Watcher needs **just enough state** to avoid spam.

Example:

* Last-seen log size
* Last alert timestamp per check

State storage:

* Local JSON file
* One per checker or one global file
* Not an API
* Not a DB

This can be added after v1 without changing architecture.

---

# 3. README (drop-in ready)

Below is a **first-pass README** you can literally paste.

---

## OPS Watcher

OPS Watcher is a lightweight operational monitoring service for QLIR.

It periodically evaluates system expectations (process presence, log growth, etc.) and emits JSON-based ops events when expectations are violated. These events are consumed by the QLIR notification server to deliver alerts via Telegram, webhooks, or other channels.

OPS Watcher is intentionally minimal and local-first. It does not expose metrics, dashboards, or APIs.

---

### Design Principles

* **Expectation-based**, not metric-based
* **Push via filesystem**, not pull via scraping
* **Emit only on anomalies**
* **Human-readable configuration**
* **No external dependencies (Prometheus, Docker, systemd)**

---

### What OPS Watcher Does

* Verifies required processes are running using kernel-exposed `proc_cmdline`
* Detects stalled or inactive log files based on growth expectations
* Emits structured JSON events to a local outbox
* Checks its own runtime presence

---

### What OPS Watcher Does Not Do

* Restart services
* Manage processes
* Collect time-series metrics
* Provide dashboards
* Replace system supervisors

---

### Configuration

OPS Watcher is configured via a TOML file.

Example:

```toml
[service]
interval_seconds = 60
emit_outbox = "/qlir/outbox/ops"

[[process_checks]]
name = "qlir_data_server"
proc_cmdline_contains = "qlir/data_server.py"

[[log_growth_checks]]
name = "analysis_log"
path = "/var/log/qlir/analysis.log"
min_growth_bytes = 1024
window_hours = 24
```

---

### Output Events

When a check fails, OPS Watcher emits a JSON event to the outbox:

```json
{
  "type": "process_missing",
  "name": "qlir_data_server",
  "proc_cmdline_contains": "qlir/data_server.py",
  "severity": "critical",
  "timestamp": "2026-01-29T14:12:03Z"
}
```

Silence indicates health.

---

### Extensibility

OPS Watcher is designed to support additional check types, such as:

* Disk usage
* Network reachability
* Heartbeat files
* Queue depth
* Resource ceilings

New checks can be added without modifying the notification server.

---

## Next logical steps (when you’re ready)

1. Define the **event schema** formally
2. Implement the scheduler loop
3. Implement process checks
4. Implement log growth checks
5. Add minimal state persistence
6. Wire into your existing notification server
