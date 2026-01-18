# QLIR Layering (MECE)

## 0) Data (I/O + Shaping)

* **What it is:** OHLCV frames, resampling, alignment, missing-data policy, timezones.
* **Examples:** `resample(df, '5m')`, `align([df1, df2])`, `fill(method='ffill')`.
* **Rule:** No finance opinions—pure plumbing.

## Core

### Ops (Deterministic, 1–2 series in → 1 series out) — *core ingredients*

Primitive math on series; this is where your “prior bar close vs this bar close” lives.

* **Pointwise transforms:** `diff(x, n=1)`, `pct_change(x, n=1)`, `log_return(x, n=1)`, `shift(x, n)`, `zscore(x, win)`.
* **Window stats:** `rolling_mean(x, win)`, `rolling_std`, `rolling_max`, `ema(x, span)`.
* **Reductions:** `cumsum`, `cummax`, `argmax/argmin`.
* **Binary ops:** `add(x,y)`, `sub`, `ratio`, `spread`.

> **Example**
> *“prior bar close to this bar close”* →
> `r1 = ops.diff(close)` (first difference)
> `r2 = ops.pct_change(close)` (percentage change)
> `dir = ops.sign(r1)` (direction −1/0/+1)

### Relations (Boolean/event primitives) — *core ingredients*

Bar-to-bar and series-to-series **relations** (pure booleans or {-1,0,1}). These are the lego bricks for everything “did X cross Y? did open rise vs prior open?”.

* **Comparators:** `gt(a,b)`, `lt(a,b)`, `ge`, `le`, `eq`, `ne`.
* **Crossovers:** `cross_up(a,b)`, `cross_down(a,b)`.
* **Bar relations:** `direction(x)` → {-1,0,+1}, `higher_high(high)`, `lower_low(low)`.
* **Range relations:** `inside(a, low, high)`, `outside(a, low, high)`.

> **Examples**
> *“this bar open relation to last bar open (direction)”* →
> `relations.direction(open)` (computed via `sign(diff(open))`)
> *“close crossed above prior high”* →
> `relations.cross_up(close, shift(high,1))`

### Counters (Stateful tallies over booleans) — *core ingredients*

Run-lengths, counts since event, bars-since—these are essential for later “episodes”.

* `streak_up(x)` / `streak_down(x)` (based on `direction(x)`).
* `count_true(cond, win)` (rolling sum of bools).
* `bars_since(cond)` (distance to last True).
* `consecutive(cond)` (run length of True).

> **Examples**
> `streak_up(close)` → how many consecutive bars up
> `bars_since(relations.cross_up(close, vwap))`


## Indicators (Named financial recipes) — *recipes, but still library-level*

Composed from Ops/Relations/Counters; canonical, parameterized, testable.

* **Bollinger:** `bb(close, win, k)` → (mid, upper, lower, width, pos)
* **MACD:** `macd(close, fast, slow, signal)` → (macd, signal, hist, slope)
* **RSI/ATR/VWAP:** pure compositions, no trade intent.

> Rule of thumb: if it has a name in TA textbooks, it’s an **indicator** (still “ingredient” for signals).


Got it — same **size, density, and cadence** as *Indicators* / *Features*.
Here’s a **tight, symmetric** section you can drop in verbatim.

---

## Column Bundles (Structural derivations) — *structure, but still library-level*

Composed from Ops / Relations / Counters / Indicators; canonical, parameterized, testable.

They introduce **structural context**—groups, legs, runs, and within-structure
coordinates—by adding **multiple, logically related columns at once**.

* **Excursion (MAE / MFE):** `excursion(df, prefix, leg_id, dir, kind)` → intra-leg idx, leg length, excursion, bps, event row, position metrics
* **Persistence / Runs:** `persistence_up_legs(df, direction_col, trendline_col)` → leg ids, run counters, per-leg persistence
* **Leg / Episode annotation:** assign group ids, mark boundaries, broadcast per-group stats

--- 

## Features (Vectorized descriptors) — *ingredients, task-agnostic*

Bundles of normalized numbers suitable for ML / ranking. No trade intent.

* **Examples:**
  `feature.bar_basic(df)` → `[pct_change(close), direction(open), hl_range, vwap_dev, bb_pos, rsi]`
  `feature.breakout_core(df)` → `%move_N`, `run_up`, `vol_ratio`, `max_dd_in_run`, etc.

> Features are *compositions* of Ops/Relations/Indicators with consistent scaling and NaN policy.

## Signals (Trade-intent logic) — *recipes, strategy-facing*

Enter/exit booleans, score functions, stop/target suggestions. Consumes Features/Indicators, emits **decisions**.

* **Examples:**
  `signal.bb_momentum_vwap(...)`
  `signal.strict_accel_breakout(...)` (later layer)
  `signal.rank_long_topk(features)` → ids/weights

---




## Naming / API sketch

```
qlir/
  core/
    ops.py            # diff, pct_change, log_return, shift, zscore, ema, rolling_*
    relations.py      # cross_up/down, direction, higher_high, inside/outside
    counters.py       # streak_up/down, bars_since, count_true, consecutive
    windows.py        # helpers for rolling/expanding state
  indicators/
    bb.py             # bb(), bb_position()
    macd.py           # macd(), macd_slope()
    rsi.py
    vwap.py
  features/
    bar_basic.py      # bar_basic(df) -> DataFrame
    breakout_core.py  # breakout primitives (pct_move_N, run stats)
  signals/
    breakout.py       # demarcation + boolean signal (no orders here)
    recipes.py        # combos like bb+vwap+accel
```

### Minimal function signatures

```python
# core/ops.py
def diff(x, n:int=1): ...
def pct_change(x, n:int=1): ...
def log_return(x, n:int=1): ...
def shift(x, n:int=1): ...
def sign(x): ...
def ema(x, span:int): ...
def rolling_mean(x, win:int): ...
def rolling_std(x, win:int): ...

# core/relations.py
def direction(x): ...                    # sign(diff(x))
def cross_up(a, b): ...                  # bool Series
def cross_down(a, b): ...
def higher_high(high): ...               # high > shift(high)
def lower_low(low): ...
def inside(x, lo, hi): ...
def outside(x, lo, hi): ...

# core/counters.py
def consecutive(cond): ...               # run-length for True
def bars_since(cond): ...
def streak_up(x): ...                    # consecutive(direction(x) > 0)
def streak_down(x): ...

# indicators/bb.py
def bb(x, win:int=20, k:float=2.0): ...  # returns mid, upper, lower, width, pos
def bb_position(x, win:int=20, k:float=2.0): ...

# features/bar_basic.py
def bar_basic(df):                       # returns DataFrame of primitives
    # cols: pct_close, dir_open, hl_range, vwap_dev, bb_pos, rsi, volume_z
    ...
```

---

## Where do your examples go?

* **“prior bar close to this bar close”** → `core.ops.diff(close)` or `core.ops.pct_change(close)`; direction via `core.ops.sign`.
* **“counts… and basic relations”** → `core.counters` for run-lengths / bars-since; `core.relations` for comparisons.
* **“this bar open relation to last bar open (direction)”** → `core.relations.direction(open)` (implemented with `sign(diff(open))`).

---

## Test scaffolding (fast to write, hard to break)

* `tests/core/test_ops.py`

  * `pct_change` monotonicity on synthetic ramp; `diff` inverse of `cumsum`.
* `tests/core/test_relations.py`

  * `cross_up` fires only on boundary; `direction` ∈ {-1,0,+1}.
* `tests/core/test_counters.py`

  * `bars_since` resets correctly; `streak_up` matches manual loops.
* `tests/indicators/test_bb.py`

  * Known values on constant series; width = 0, pos = 0.5.

---

## TL;DR

* **Ingredients**: `data → ops → relations → counters → indicators → features`
* **Recipes**: `signals` (anything that implies *trade intent*).
  “bar-to-bar” diffs, directions, and counts are **core primitives** (Ops/Relations/Counters), not indicators. 
  Indicators are named compositions;
  features are task-agnostic bundles; 
  signals are the trading logic.
