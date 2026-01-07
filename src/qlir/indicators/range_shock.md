# Range Shock

## Motivation

`range_shock` exists to detect **abnormal singlebar (or intrabar) volatility expansion**.

Where ARP describes *what is normal lately*, range shock answers:

> *Did something extreme just happen relative to recent behavior?*

This makes `range_shock` an **event detector**, not a descriptive volatility summary.

---

## Core Definition

For each bar *i*:

```
range_pct[i] = (high[i] - low[i]) / open[i]
baseline[i]  = ARP(range_pct, window) evaluated at i-1
range_shock[i] = range_pct[i] / baseline[i]
```

The output is a **dimensionless ratio**.

---

## Interpretation

| Value | Meaning                         |
| ----- | ------------------------------- |
| 1.0   | Normal intrabar range           |
| 2.0   | 2× normal expansion             |
| 3.0+  | Regime break / liquidation risk |
| 5.0+  | Structural volatility event     |

Values are **multiples of normal**, not percentages.

Example:

* `range_shock = 1.2` → 120% of normal
* `range_shock = 3.0` → 3× normal

---

## Baseline Choice

The baseline for range shock is **ARP**, not a raw rolling statistic.

Reasons:

* ARP already defines *recent normal* in percent terms
* ARP evolves in wave-like regimes
* Using ARP makes shock detection regime-aware by construction

The baseline is always **shifted** to avoid self-reference.

---

## Design Properties

* **Causal**: no lookahead
* **Asymmetric**: current vs past
* **Scale-free**: invariant to price level
* **Event-oriented**: suitable for thresholding and isolation

`range_shock` is intentionally narrow in scope.

---

Yes — that distinction is real, important, and worth encoding **explicitly** in the Core Definition.
You don’t need a new doc or a new concept — just a **temporal correctness clause**.

Here’s a clean way to rewrite / extend that section without overcomplicating it.

---

## Core Definition (with temporal semantics)

For each bar *i*:

```
range_pct[i]   = (high[i] - low[i]) / open[i]
baseline[i]    = ARP(range_pct, window) evaluated at i-1
range_shock[i] = range_pct[i] / baseline[i]
```

The output is a **dimensionless ratio**.

---

## Relationship to ARP

| Metric      | Role                               |
| ----------- | ---------------------------------- |
| ARP         | Describes normal intrabar activity |
| Range Shock | Detects abnormal expansion         |

These metrics are **orthogonal** and should not be conflated.

---

## What Range Shock Does Not Encode

* Direction (use directional ARP for that)
* Distributional shape inside the window
* Duration or decay of volatility

Those concerns belong to **separate studies**.

---

## Design Invariants

* No semantic overloading
* No hidden thresholds
* No baked-in distribution assumptions
* One event, one signal

If you need distributional certainty, study the distribution explicitly.


### Temporal Semantics (Important)

There are two distinct use cases for `range_shock`, and they differ only in **when the baseline is allowed to see data**.

#### 1. Realtime Event Detection (Execution-Safe)

For realtime detection, the baseline **must not include bar *i***.

* `baseline[i]` is computed from bars `[i-window, …, i-1]`
* Including bar *i* would leak `high[i]` / `low[i]`
* This would make the shock detectable **only after the bar has closed**

**Invariant:**

> For realtime use, the ARP baseline MUST end at *i-1*.

This ensures `range_shock[i]` can be evaluated immediately at bar close, without self-reference.

---

#### 2. Historical Event Studies (Post Hoc Analysis)

For historical analysis, the timing constraint may be relaxed **intentionally**:

* baselines may include bar *i*
* or be recomputed symmetrically
* or use centered / smoothed variants

This can be useful for:

* regime labeling
* clustering events
* visualization
* retrospective studies

However, such variants are **not execution-safe** and should not be used for realtime detection.

---

### Summary Rule

* **Realtime detection** → baseline ends at *i-1* (mandatory)
* **Historical studies** → baseline timing is a modeling choice

The formula is the same; only the **temporal alignment** differs.

---

This wording does a few important things:

* makes the realtime constraint non-negotiable
* explains *why* (high/low availability)
* doesn’t overformalize with extra symbols
* keeps both use cases legitimate but clearly separated

If you want, the next refinement would be to add a single-line note like:

> “Implementations should default to execution-safe alignment.”

—but even without that, this section is now doing the right job.

