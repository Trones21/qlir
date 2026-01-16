# Bucketization

This module defines **bucketization** as a first-class statistical operation and makes an explicit distinction between **lossless** and **lossy** bucketization.

Bucketization is used throughout QLIR to convert raw values into grouped representations for counting, distributions, and downstream analysis. The key design rule is that **lossiness must always be intentional and explicit**.

---

## Core Concepts

### Lossless Bucketization

**Lossless bucketization** preserves all information in the original variable.

**Properties**
- Each value maps to exactly one bucket
- Mapping is invertible
- Ordering is preserved
- No arbitrary boundaries
- No information loss

In practice, this means **each unique value is its own bucket**.

**Typical use cases**
- Integer durations (e.g. run length, persistence)
- Discrete time indices (bars)
- Categorical states or IDs
- Exact regime labels

**Example**
```text
run_length ∈ {1, 2, 3, 4, 5}
bucket = run_length
````

Lossless bucketization is not approximation — it is **representation**.

---

### Lossy Bucketization

**Lossy bucketization** intentionally collapses multiple values into ranges.

**Properties**

* Information is destroyed
* Mapping is not invertible
* Intra-bucket shape is hidden
* Bucket boundaries are subjective

**Typical use cases**

* Percentiles / quantiles
* Price or volatility bands
* Regime compression
* Human-friendly summarization

**Example**

```text
run_length ∈ [1–6] → bucket_1
```

Lossy bucketization is **summarization**, not structure.

---

## Survival vs Bucketization

Survival analysis operates on **lossless buckets**.

In discrete time systems (such as bar-based market data), survival distributions are computed by:

1. Losslessly bucketizing event durations (one bucket per integer)
2. Counting events per duration
3. Computing cumulative tail probabilities

Although survival analysis uses buckets, it is **semantically distinct** from generic bucketization:

* Bucketization answers: *“How many observations fall into each bucket?”*
* Survival answers: *“What is the probability an event survives to duration k?”*

For this reason, survival-related utilities live in `distributions.survival`, not here.

---

## Design Notes

Bucketization exists on a spectrum from **lossless** to **lossy**.  
Neither is inherently “better” — they serve different purposes at different stages of analysis.

### Lossless vs Lossy in Practice

- **Lossless bucketization** preserves full granularity and is appropriate when:
  - the exact shape of a distribution matters
  - ordering and tail behavior are important
  - results will be reused downstream (e.g. survival analysis)

- **Lossy bucketization** provides a coarser view and is often useful when:
  - performing early exploratory analysis
  - reducing cognitive or visual complexity
  - answering high-level questions quickly

Using lossy buckets can be a deliberate way to obtain a rough understanding **before** moving to a finer, lossless view.

---

### Persistence & Survival

Persistence analysis often begins with lossy summaries (e.g. broad duration ranges) to understand overall structure.

When more precision is needed, the same data can be re-examined using lossless bucketization to compute exact survival or tail distributions.

In this sense:
- lossy bucketization offers **coarse intuition**
- lossless bucketization supports **precise measurement**

These approaches are complementary, not mutually exclusive.

---

### API & Documentation Guidance

- Functions should make clear whether bucketization is lossless or lossy
- Lossy behavior should be visible in parameters or naming
- Lossless outputs are preferred when results are intended for reuse or composition

This module provides utilities for both, leaving the choice of granularity to the analysis stage.

---

## Summary

* **Lossless bucketization** preserves structure and truth
* **Lossy bucketization** is a compression layer

This distinction is foundational to QLIR’s statistical design philosophy.

