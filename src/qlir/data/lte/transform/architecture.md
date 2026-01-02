# Gap Handling Architecture

This document explains the **architectural model and design decisions** behind the gap-handling pipeline.

It is intended for:

* future maintainers
* reviewers
* researchers auditing assumptions
* future-me

This document focuses on **why**, not **how**.

---

## Core Problem Statement

Time-series market data has two fundamentally different kinds of “missingness”:

1. **Missing time**
   Time passed, but no row exists (and i mean all clock time... unix time... it doesnt matter if the instrument was trading)

2. **Missing values**
   Time passed, a row exists, but prices were not observed.

Most systems conflate these.
This module **does not**.

---

## Foundational Principle

> **Missing prices ≠ missing time**

If time passed, there should be a row. Full stop.

This principle drives the entire architecture.

---

## Phase-Based Architecture

The pipeline is intentionally split into **strict phases** with hard boundaries.

### Phase 0 — Raw input (out of scope)

* timestamps may be columns
* time may be sparse
* semantics are unclear

---

### Phase 1 — Time Materialization

**Responsibility:**
Make elapsed time explicit.

**Module:**

```
transform/gaps/materialization/materialize_missing_rows
```

**Guarantees after this phase:**

* DataFrame is indexed by `DatetimeIndex`
* Index represents **bar-open timestamps**
* Time is dense at a fixed wall-clock interval
* Rows where time passed but price was unobserved are explicitly marked

**Non-goals:**

* No price filling
* No interpolation
* No semantic interpretation of gaps

This phase is **purely ontological**: it answers *“did time pass?”*

---

### Phase 2 — Gap Structure Analysis

**Responsibility:**
Identify where value materialization *could* occur.

**Modules:**

```
transform/gaps/blocks.py
transform/gaps/context.py
```

**Key ideas:**

* Missing rows are grouped into contiguous blocks
* Each block is validated against strict invariants
* Safe boundary conditions are enforced

**Critical invariant:**

> No gap is filled unless it has **real observations on both sides**.

This phase prevents silent extrapolation.

---

### Phase 3 — Value Materialization (Fill Policies)

**Responsibility:**
Materialize prices under **explicit, declared assumptions**.

**Modules:**

```
transform/gaps/materialization/apply_fill_policy
transform/policy/*
```

**Key properties:**

* Policies are explicit objects
* Policies declare their context requirements
* Synthetic rows are tagged and auditable
* Observed data is never overwritten

This phase answers *“what do we assume price might have done?”*

---

## The FillContext Contract

`FillContext` is the **single contract** between structure and semantics.

It contains:

* left and right real boundary candles
* the exact timestamps to fill
* a bounded, real-only local context window (optional)
* no access to the full DataFrame

**Important rule:**

> Policies consume facts.
> They do not traverse structure.

This keeps semantics isolated from storage concerns.

---

## Local Context Windows

Context windows are:

* symmetric
* bounded
* real-only
* explicitly requested by policies

They are **not** volatility-specific.

They may be used for:

* volatility estimation
* trend inference
* regime classification
* envelope shaping
* confidence scoring
* rejection logic

The architecture treats context as **general local information**, not a single metric.

---

## Determinism & Research Integrity

This architecture guarantees:

* deterministic outputs (unless a policy explicitly opts into randomness)
* reproducibility
* explicit tagging of synthetic data
* traceability from result → policy → assumptions

If a backtest benefits from synthetic data, that fact is visible and auditable.

---

## What This Architecture Intentionally Avoids

* Implicit interpolation
* Heuristic gap detection
* Silent extrapolation at dataset boundaries
* Policy logic embedded in ETL code
* Conflating time correctness with value assumptions

These are common sources of research error.

---

## Mental Model Summary

```text
Raw data
   ↓
Time materialization (time exists)
   ↓
Gap structure (where filling is allowed)
   ↓
Explicit policies (how price is assumed)
```

At every stage, the system answers a **different question**.

---

## Design Philosophy (TL;DR)

* Make time explicit
* Make assumptions explicit
* Make violations loud
* Make semantics opt-in
* Never lie quietly
