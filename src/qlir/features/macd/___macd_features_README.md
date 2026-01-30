Got it — succinct, but still *structural*, not hand-wavy.
Here’s a **clean README** you can drop straight into `features/macd/README.md`.

---

# MACD Feature Module

This module provides **structural, event-level features** derived from the MACD histogram.
The goal is not indicator plotting, but **explicit segmentation, transitions, and extrema** that can be analyzed statistically.

All functions operate on precomputed MACD / signal columns and return **annotated, composable features**.

---

## Core Concepts

### Histogram

* `macd_hist = macd - macd_signal`
* Sign defines regime (green / red)
* Magnitude defines excursion (energy)

### Colors

Histogram bars are classified into four states:

* `dark_green`  → expanding bullish
* `light_green` → contracting bullish
* `dark_red`    → expanding bearish
* `light_red`   → contracting bearish

These states are the foundation for all higher-level structure.

---

## Feature Layers

### 1. Crosses & Regimes

**Purpose:** define segment boundaries.

* `macd_cross_up`
* `macd_cross_down`
* `macd_hist_positive`
* `macd_hist_negative`

Used to segment data **between crosses** and anchor transitions.

---

### 2. Histogram Distance & Coloring

**Purpose:** make expansion / contraction explicit.

Outputs:

* signed distance
* absolute distance
* color state (string or int encoding)

This layer feeds pyramids, extrema, and transition logic.

---

### 3. Pyramids (Structure Within Segments)

#### Loose pyramids

* Defined **between crosses**
* Must contain both dark and light bars
* Allows internal reversals (dark → light → dark)
* Captures *structured but noisy* moves

Wrappers:

* `loose_green`
* `loose_red`

#### Strict pyramids

* Enforces monotonic expansion → contraction
* No internal reversals

Wrappers:

* `strict_green`
* `strict_red`

---

### 4. Strict Crossing Sequences (Transitions)

These detect **clean regime handoffs**.

Two variants:

#### Strict crossings (general)

* `light → dark` across a sign change
* No color mixing
* Does **not** require extrema anchoring

#### Strict extrema crossings

* Must start and end at histogram extrema
* Subset of strict crossings
* Cleanest regime transitions

Relationship:

```
strict_extrema ⊂ strict_crossing
```

---

### 5. Segment Excursion Extremes

**Purpose:** mark where energy peaks within a segment.

* Operates on signed or absolute values
* One max excursion per segment
* Only for segments with length ≥ 3

Used for:

* MFE / MAE timing
* excursion asymmetry
* outcome conditioning

---

## Design Principles

* **No implicit TA semantics**
* **Explicit state machines**
* **No lookahead**
* **Deterministic, testable logic**
* **Composable primitives**

This module is intended to support **statistical analysis**, not discretionary charting.

---

## Typical Usage Flow

1. Compute MACD + signal
2. Add histogram & cross flags
3. Add histogram coloring
4. Detect pyramids and crossings
5. Mark excursion extrema
6. Condition outcomes on structure

---

## Summary

This MACD features module treats the indicator as a **stateful process**, not a line on a chart.

It exposes:

* regimes
* transitions
* structure
* extrema

…as **first-class data**, suitable for rigorous analysis.
