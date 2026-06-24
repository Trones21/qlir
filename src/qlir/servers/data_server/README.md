# Data Server

## Background: the data server is *two* processes

Although it's launched as one service (`poetry run data_server`), the "data server" actually
runs **two cooperating processes**:

1. **The Fetcher** — the data-fetch / ingest loop. It pulls klines from Binance and writes raw
   response slices to disk (`responses/<slice_id>.json`). This is the hot loop; it must never
   stall.
2. **The Manifest Builder** — applies the delta log to materialize the authoritative index
   (`manifest.json`) of which slices exist and their status.

> **Naming note.** These two names describe *roles*. Earlier notes (and some code symbols) call
> the Fetcher the *worker* and the Manifest Builder the *aggregator* / *delta-log service* —
> but "worker" wrongly implies a background helper (this is the main loop), and "aggregator"
> collides with the separate **aggregation service** (`agg_server`). They are different things:
> the Manifest Builder lives *inside* the data server and produces `manifest.json`; the
> aggregation service is a downstream stage that *consumes* `manifest.json`.

### Why they were split

The manifest is a single index over *every* slice ever fetched. For **1-second** data it grows
to **hundreds of megabytes**. If the Fetcher had to load, validate, and rewrite that file on
every loop, the cost of touching the manifest would **block ingestion** — the Fetcher would
spend its time serializing a giant JSON file instead of fetching candles.

So manifest materialization was moved into its **own process** (the Manifest Builder), and the
two are coordinated **through on-disk artifacts rather than locks**:

- the Fetcher only ever *describes observed state* — it writes `manifest.snapshot.json` and
  appends to `manifest.delta`;
- the Manifest Builder only ever *materializes authoritative state* — it owns `manifest.json`.

This keeps the fetch loop non-blocking, with **no shared mutexes and no deadlocks**, even as
the manifest balloons. The strict ownership rules below are what make that split safe — they
are the whole reason this document exists.

### Where the downstream aggregation service fits

The aggregation service (`agg_server`) reads **only the authoritative `manifest.json`** — it
never reads `manifest.delta`. The delta log is a private coordination channel *between the two
data-server processes*; by the time the aggregation service sees anything, the Manifest Builder
has already materialized a coherent `manifest.json`. That's why a downstream consumer never
observes a partial or mid-write index.

---

# Manifest Coordination & Ownership

## At a glance: the three files

| File | Purpose | Writer / owner | Reader(s) |
|---|---|---|---|
| `manifest.snapshot.json` | A point-in-time view of *observed filesystem reality*, used as the startup handshake so the Manifest Builder begins from a complete, coherent state. | **Fetcher** | Manifest Builder |
| `manifest.delta` | Append-only log of incremental slice-status changes the Fetcher observes during steady-state operation. | **Fetcher** | Manifest Builder |
| `manifest.json` | The authoritative, materialized index of every slice and its status. Derived state — safe to delete and rebuild. | **Manifest Builder** | downstream `agg_server` (**never** the Fetcher) |

Two principles make this deadlock-free, and they're the whole point of the rules below:

1. **Single-writer per file** — every file has exactly one writer, so there is never write-write
   contention and never a need for a shared lock/mutex. (Three files, two processes: the Fetcher
   owns two of them, the Manifest Builder owns one.)
2. **Disjoint write/wait sets** — no process both *waits on* and *writes* the same file. The
   Fetcher writes snapshot+delta and waits on nothing it writes; the Manifest Builder waits on
   the snapshot (which it never writes) and writes `manifest.json` (which it never waits on).
   That acyclic relationship is exactly what rules out the circular-dependency deadlock.

## ❗ Hard Rule (Non-Negotiable)

**The Fetcher MUST NEVER write to `manifest.json`.**

* The Fetcher may only write:

  * `manifest.snapshot.json`
  * `manifest.delta`
* Only the **Manifest Builder** may write:

  * `manifest.json`

Any violation of this rule will cause deadlocks, startup races, or circular dependencies.

---

## Why This Rule Exists

The manifest is **not a source of truth**.

* **Raw response files on disk are the source of truth**
* The manifest is *derived state*
* It must be safe to delete and rebuild at any time

Introducing `manifest.snapshot.json` makes ownership explicit:

* The Fetcher **describes observed state**
* The Manifest Builder **materializes authoritative state**

This separation avoids:

* circular startup dependencies
* partial manifest writes
* cross-process locking
* hidden authority transfer via timing

---

## Design Goal: Parallel, Non-Blocking Processes

Unlike a single global file lock, this design allows:

* The Fetcher and Manifest Builder to run **in parallel**
* Each process to make progress **when it can**
* Coordination via **artifacts**, not mutexes

No process ever blocks while *holding* authority.

---

## Startup & Runtime Flow

### 1. Fetcher Startup

On startup, the Fetcher **explicitly resets derived state**:

* Deletes:

  * `manifest.json`
  * `manifest.delta`
  * `manifest.snapshot.json`

This guarantees no stale or partially-written state survives restarts.

---

### 2. Fetcher Rebuilds Snapshot (If Needed)

The Fetcher:

1. Enumerates expected slices from wall-clock time
2. Scans the raw response filesystem
3. Rebuilds an in-memory manifest
4. Writes **only** `manifest.snapshot.json`

> The snapshot represents *observed filesystem reality*, not authority.

The Fetcher **never** loads or writes `manifest.json`.

---

### 3. Manifest Builder Waits for Snapshot

The Manifest Builder:

* Blocks until `manifest.snapshot.json` exists and is non-empty
* Loads the snapshot
* Assumes ownership of manifest materialization

This guarantees the Manifest Builder always starts from a complete, coherent view. (This is the
specialized startup handshake that synchronizes the two processes to the same state before
steady-state operation begins.)

---

### 4. Manifest Builder Owns `manifest.json`

Once running, the Manifest Builder:

* Applies deltas
* Maintains `manifest.json`
* Writes updates atomically
* Treats snapshots as input, never output

From this point on:

* `manifest.json` is authoritative
* `manifest.snapshot.json` is disposable

---

## Key Invariants

* `manifest.json` may be deleted at any time
* Snapshots must be derivable from filesystem state
* No process both **waits on** and **writes** the same artifact
* Coordination happens via files, not locks

---

## Common Failure Mode (What This Prevents)

❌ Fetcher writes `manifest.json`
❌ Manifest Builder waits on `manifest.json`
❌ Circular dependency → deadlock

This README exists specifically to prevent that class of bug.

---

## Summary

* **Filesystem = truth**
* **Fetcher = observer** (writes snapshot + delta, never `manifest.json`)
* **Manifest Builder = authority** (applies the delta log, owns `manifest.json`)
* **Snapshots = coordination primitive**

If you're unsure which file to write to:

> If it's `manifest.json`, the answer is "no" unless you are the Manifest Builder.


## Startup flow tldr

### Fetcher

```text
delete manifest.json
delete manifest.delta
delete manifest.snapshot

↓
enumerate expected slices
↓
load_or_create_manifest (empty)
↓
if responses exist → rebuild from filesystem
↓
write snapshot
↓
wait for Manifest Builder to move snapshot → reload

continue
```

### Manifest Builder (applies the delta log)

```text
wait until manifest.snapshot.json exists and non-empty
↓
load snapshot
↓
begin applying deltas → manifest.json
```
