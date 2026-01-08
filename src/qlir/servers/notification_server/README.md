Great idea — this is exactly the moment to write it, while the scope is still *honest and small*.

Below is a **README.md** you can drop straight into
`qlir/servers/notification/README.md` (or equivalent).

It deliberately:

* avoids semantics
* avoids UX promises
* documents only **what exists**
* sets expectations for future extension without committing to it

---

# QLIR Notification Server

The **QLIR Notification Server** is a long-running process responsible for **delivering alert payloads outward**.

It does **not** decide *what* an alert means.
It does **not** evaluate conditions.
It does **not** format alerts.

It simply takes alert records produced elsewhere and **pushes their payloads to external endpoints**.

---

## What this server does

* Watches a directory (`alerts/outbox/`) for alert files
* For each alert file:

  * Loads the alert JSON
  * Extracts the `data` field
  * Sends `data` to one or more outbound adapters (e.g. webhooks)
* On success:

  * Moves the file to `alerts/sent/`
* On repeated failure:

  * Moves the file to `alerts/failed/`

This server is **push-only** and **outbound-only**.

---

## What this server does *not* do

* No alert semantics (severity, importance, urgency)
* No filtering or throttling
* No deduplication
* No alert formatting
* No condition evaluation
* No dashboards or UIs

Those concerns live elsewhere (or later).

---

## Alert file contract

Alert files are JSON objects with a **minimal envelope**:

```json
{
  "ts": "2026-01-08T23:15:02Z",
  "data": { ... }
}
```

### Fields

* `ts`
  Timestamp indicating when the alert was emitted.

* `data`
  An **opaque payload**.
  This can be any valid JSON value (object, string, number, etc.).

The notification server does **not interpret** `data`.
It forwards it as-is to outbound adapters.

---

## Directory layout

```
alerts/
  outbox/   # new alerts written by analysis server
  sent/     # alerts successfully delivered
  failed/   # alerts that exceeded retry limits
```

The filesystem acts as the queue and the state store.

---

## Execution model

The notification server runs as a **single infinite loop**:

1. Scan `alerts/outbox/` for `.json` files
2. Attempt to deliver each alert
3. Move files based on outcome
4. Sleep for a short interval
5. Repeat

It is safe to:

* restart the process
* crash the process
* stop the process temporarily

No alerts are lost as long as files remain on disk.

---

## Adapters

Outbound delivery is handled via **adapters**.

An adapter is a small class that implements:

```python
def send(data: Any) -> None
```

Adapters are expected to:

* perform a side effect (HTTP request, message send, etc.)
* raise an exception on failure

Examples:

* Webhook adapter (HTTP POST)
* Telegram bot adapter
* Slack webhook adapter
* SNS adapter (future)

Adapters are configured in the notification server and are invoked sequentially.

---

## Retry behavior

* Each alert may be retried a small number of times
* Retry count is stored in alert file metadata
* Alerts that exceed retry limits are moved to `alerts/failed/`

Retry logic is intentionally minimal and conservative.

---

## Observability

The notification server:

* Emits structured logs
* Uses the filesystem as an observable state

You can inspect system state with standard tools:

```bash
ls alerts/outbox
ls alerts/sent
ls alerts/failed
```

---

## Design philosophy

This server follows a few strict principles:

* **Boring over clever**
* **Durable over fast**
* **Push over pull**
* **Filesystem over IPC**
* **Structure before semantics**

The goal is to establish a reliable delivery pipe before layering meaning on top.

---

## Future extensions (explicitly out of scope for now)

These are intentionally **not implemented yet**:

* Alert severity levels
* Notification batching or digests
* Formatting or templating
* Conditional routing
* Multi-tenant support

The current design keeps these options open without committing to them.

---

## Summary

The QLIR Notification Server is a small, reliable, outbound-only server that:

> Takes alert payloads from disk
> and pushes them outward.

Nothing more. Nothing less.

