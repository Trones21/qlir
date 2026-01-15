# Manifest Coordination & Ownership

## ❗ Hard Rule (Non-Negotiable)

**The worker process MUST NEVER write to `manifest.json`.**

* The worker may only write:

  * `manifest.snapshot.json`
  * `manifest.delta`
* Only the **aggregator / delta-log service** may write:

  * `manifest.json`

Any violation of this rule will cause deadlocks, startup races, or circular dependencies.

---

## Why This Rule Exists

The manifest is **not a source of truth**.

* **Raw response files on disk are the source of truth**
* The manifest is *derived state*
* It must be safe to delete and rebuild at any time

Introducing `manifest.snapshot.json` makes ownership explicit:

* Workers **describe observed state**
* Aggregator **materializes authoritative state**

This separation avoids:

* circular startup dependencies
* partial manifest writes
* cross-process locking
* hidden authority transfer via timing

---

## Design Goal: Parallel, Non-Blocking Processes

Unlike a single global file lock, this design allows:

* Worker and aggregator to run **in parallel**
* Each process to make progress **when it can**
* Coordination via **artifacts**, not mutexes

No process ever blocks while *holding* authority.

---

## Startup & Runtime Flow

### 1. Worker Startup

On startup, the worker **explicitly resets derived state**:

* Deletes:

  * `manifest.json`
  * `manifest.delta`
  * `manifest.snapshot.json`

This guarantees no stale or partially-written state survives restarts.

---

### 2. Worker Rebuilds Snapshot (If Needed)

The worker:

1. Enumerates expected slices from wall-clock time
2. Scans the raw response filesystem
3. Rebuilds an in-memory manifest
4. Writes **only** `manifest.snapshot.json`

> The snapshot represents *observed filesystem reality*, not authority.

The worker **never** loads or writes `manifest.json`.

---

### 3. Aggregator Waits for Snapshot

The aggregator / delta-log service:

* Blocks until `manifest.snapshot.json` exists and is non-empty
* Loads the snapshot
* Assumes ownership of manifest materialization

This guarantees the aggregator always starts from a complete, coherent view.

---

### 4. Aggregator Owns `manifest.json`

Once running, the aggregator:

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

❌ Worker writes `manifest.json`
❌ Aggregator waits on `manifest.json`
❌ Circular dependency → deadlock

This README exists specifically to prevent that class of bug.

---

## Summary

* **Filesystem = truth**
* **Worker = observer**
* **Aggregator = authority**
* **Snapshots = coordination primitive**

If you’re unsure which file to write to:

> If it’s `manifest.json`, the answer is “no” unless you are the aggregator.


## Startup flow tldr

## Worker 

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
wait for aggregator to move snapshot → reload

continue
```

### Delta / Aggregator Service

```text
wait until manifest.snapshot.json exists and non-empty
↓
load snapshot
↓
begin aggregation
```
