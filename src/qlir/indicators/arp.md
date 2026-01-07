# ARP — Average Range Percent

## Motivation

ARP exists to measure **intrabar volatility intensity** in a way that is:

* execution-safe
* window-local
* price-level invariant
* semantically unambiguous

It deliberately avoids overloading or spoofing Wilder-style ATR semantics.

---

## Base Definition: ARP

For each bar *i*:

```
range_abs[i] = high[i] - low[i]
range_pct[i] = range_abs[i] / open[i]
arp[i]       = SMA(range_pct, window)
```

### Design choices

* **Intrabar only**: uses `high - low`, no gap logic
* **Same-bar normalization**: divides by `open[i]`
* **Percent first, average second**: averaging is done on a dimensionless series
* **No lookahead**: all inputs are known by bar close

ARP answers the question:

> *How much price exploration is happening inside a bar, relative to price?*

---

## Why ARP Is Not ATR

ARP intentionally does **not** use True Range:

* no `close[i-1]`
* no gap expansion
* no cross-bar dependency

If you need Wilder-style ATR or ATRP, that is a **different indicator** and should not reuse this name.

---

## Directional Decomposition (ARP-D)

Intrabar volatility is not directionless. A small counter-direction bar can penalize a strong impulse if ranges are averaged blindly.

Example:

```
large green bar
small red bar (micro pullback)
large green bar
```

To preserve signal, ARP is decomposed into **directional components**.

---

## Directional Primitives

### Bar Direction

```
dir[i] = sign(close[i] - open[i])
```

* `dir > 0` → bullish bar
* `dir < 0` → bearish bar
* `dir = 0` → doji (indecision)

Dojis are excluded from directional metrics.

---

### ARP⁺ — Bullish Range Intensity

```
arp_pos = mean(range_pct[i] | close[i] > open[i])
```

**Window semantics**:

* filtering happens **before** averaging
* the window is over *matching bars*, not time

This answers:

> *When bullish bars occur, how large are they on average?*

---

### ARP⁻ — Bearish Range Intensity

```
arp_neg = mean(range_pct[i] | close[i] < open[i])
```

Symmetric to ARP⁺.

---

### Count⁺ — Bullish Frequency

```
arp_pos_count = count(close > open) / window
```

Answers:

> *How often is pressure upward?*

---

### Count⁻ — Bearish Frequency

```
arp_neg_count = count(close < open) / window
```

Answers:

> *How often is pressure downward?*

---

## ARP-D Wrapper

`arp_d` is a **convenience wrapper** that returns all directional components in one call:

* `arp_pos`
* `arp_neg`
* `arp_pos_count`
* `arp_neg_count`

It contains no independent logic.

The `-d` suffix mirrors Unix-style directional flags.

---

## Why Averages Collapse Structure (and What ARP Does *Not* Preserve)

Averages alone collapse structure.

Consider two windows with the same mean range percent:

**Window A**

```
10  12  9  10
```

**Window B**

```
20   0   2  20
```

Both sum to 42 over 4 bars → the same average.

Yet they describe very different microstructure:

* **Window A**: consistent, stable pressure
* **Window B**: impulsive spikes separated by compression

This difference **cannot be recovered from the mean alone**.

Importantly, *directional counts do not solve this problem either*.

Even if the small bars in Window B are directionally aligned (e.g. small green pullbacks inside a larger green impulse), the average range percent still collapses:

* clustering
* tail dominance
* impulse vs compression structure

No combination of simple averages and counts can uniquely encode the full microstructure of a window.

This is an **information-theoretic limitation**, not a modeling oversight.

ARP (and ARP-D) therefore make an explicit tradeoff:

* they remember **how large bars are on average**
* they remember **how often directions occur**
* they deliberately forget **the internal distributional shape**

Many different input shapes can map to the same ARP value.

---

## Recovering Distributional Context (Separate Concern)

If distributional certainty matters, the correct approach is **not** to overload ARP, but to place each bar (or window statistic) inside a **much larger empirical distribution**.

Concretely:

1. Compute and persist the primitive series:

   ```
   range_pct = (high - low) / open
   ```

2. Build a large reference distribution over:

   * hundreds to millions of rows
   * possibly conditioned by regime, session, or instrument

3. Compute distributional statistics such as:

   * percentile / quantile rank
   * long-run standard deviation
   * tail probability

This is a **separate study**, operating over a *vertical* slice of data, not a rolling window.

---

## Joining Distributional Context Back to ARP

The results of such a study can be joined back into the base DataFrame to provide context:

| timestamp | arp   | range_pct | range_pct_std_1y | range_pct_pctl_1y |
| --------- | ----- | --------- | ---------------- | ----------------- |
| t₁        | 0.032 | 0.041     | 0.015            | 0.92              |
| t₂        | 0.029 | 0.018     | 0.015            | 0.47              |
| t₃        | 0.035 | 0.062     | 0.016            | 0.98              |

Here:

* `arp` describes **local average activity**
* `range_pct_std_1y` describes **long-run dispersion**
* `range_pct_pctl_1y` locates the current bar inside a **large-sample distribution**

This separation preserves clarity:

* ARP remains a clean rolling indicator
* distributional shape is modeled explicitly
* no information is silently destroyed

If you need distributional structure, model a distribution.
If you need a scalar indicator, accept that aggregation forgets.

---

## A note about window distribution stats...

ARP intentionally encodes only **first-order statistics**:

* mean range (via ARP / ARP⁺ / ARP⁻)
* frequency (via Count⁺ / Count⁻)

It does **not** attempt to encode the full window distribution on each row.

Over the life of a window, the following information also exists:

* min range
* max range
* median
* full empirical distribution

Collapsing all of that into a single scalar (e.g. standard deviation) is often misleading, especially for small windows (e.g. 14 bars), where variance estimates are unstable.

In QLIR, distributional information is treated as a **separate concern**:

* available via the raw `range_abs` / `range_pct` series
* explored through bucketization, quantiles, or regime studies
* not silently baked into scalar indicators

If you need higher-order structure, it should be **explicitly modeled**, not hidden inside ARP.

While I considered including win_max, win_min, win_median, this just feel messy.. im not surei can connect it to a use case... if a say win max is desired, then just do a max over the range_pct col() and pply your chosen window 

---

## Interpretation Cheat Sheet

| Metric | Meaning                        |
| ------ | ------------------------------ |
| ARP    | Overall intrabar activity      |
| ARP⁺   | Strength of bullish impulses   |
| ARP⁻   | Strength of bearish impulses   |
| Count⁺ | Directional persistence (up)   |
| Count⁻ | Directional persistence (down) |

---

## Design Invariants

* No lookahead
* No global anchors
* No semantic overloading
* One metric per function
* Wrappers compose primitives

If a future change violates one of these, it belongs in a **new indicator**, not here.
