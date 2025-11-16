# Canonical Instruments: The Foundation of QLIR’s Identity Layer

**Status:** Core architectural concept  
**Scope:** Data, research pipelines, storage, indicators, SPL integration  
**Audience:** Anyone extending QLIR or SPL with new instruments or venue adapters

---

## 🔥 1. What Is a Canonical Instrument?

A **Canonical Instrument** is QLIR’s *internal*, stable, human-readable identity
for a tradable asset.

Examples:

- `CanonicalInstrument.SOL_PERP`
- `CanonicalInstrument.BTC_PERP`
- `CanonicalInstrument.BONK_PERP`

This identifier is *venue-agnostic*. It does **not** reflect how any exchange labels
the instrument.

It is the **root identity** for:

- QLIR’s research side (data, indicators, stats, universes), and
- SPL’s strategy/execution side, which imports and uses this identity layer.

---

## 🔗 2. Why Canonical Instruments Exist

Without a canonical identity layer, you end up with:

- stringly-typed bugs (`"S0L-PERP"` accidentally becoming a “new” asset)
- fragile symbol inference
- per-venue inconsistencies
- unstable filenames
- brittle multi-venue support

Canonical instruments solve this by giving both **QLIR (research)** and **SPL (strategy)**
a shared, stable vocabulary.

---

## 🧭 3. What “QLIR Support” Actually Means (Today)

When **QLIR** “supports” an instrument, that currently means:

1. A `CanonicalInstrument` enum entry exists  
2. There is a mapping between:

   ```text
   CanonicalInstrument  <->  venue-native ID
````

via a `SymbolMap` (e.g. `DriftSymbolMap`, `HeliusSymbolMap`)

3. Standardized disk paths can be generated for it
4. Research pipelines, discovery, diffs, and tooling can operate safely on it

**It does *not* yet guarantee:**

* that historical data actually exists on disk,
* that any particular SPL strategy trades it,
* that risk configs have been tuned for it.

Right now, **“QLIR support” = identity + mapping + naming + structural guarantees**.
SPL builds on top of that to decide *what* and *how* to trade.

---

## 📁 4. Deterministic Storage for Research

Once canonicalized, QLIR guarantees:

* clean file naming, e.g.:

  ```text
  sol-perp_1m.parquet
  sol-perp_15m.parquet
  ```

* deterministic folder structure

* no string inference, no heuristics, no guessing

This matters because QLIR’s research pipelines and indicator computations assume
they can derive paths from `(CanonicalInstrument, TimeFreq)` without any parsing hacks.

SPL can then rely on QLIR’s outputs (e.g. precomputed indicators) being organized
consistently by canonical instrument.

---

## 🗺️ 5. Multi-Venue Join Key

A canonical ID unifies data across all venues:

```text
SOL_PERP (canonical)
 ├── Drift:   "SOL-PERP"
 ├── Helius:  MarketId("SOL", "perp")
 ├── Kaiko:   ("sol", "perp")
 └── Mock:    FakeMarket("SOL-PERP")
```

Q﻿LIR uses this to:

* normalize multi-venue research, and
* allow SPL to reason about “SOL_PERP” as a single concept,
  even if execution can route to different venues or data providers.

---

## 🧩 6. Canonical Universes (For Research & Strategies)

Because instruments are canonical, QLIR can define universes like:

* `MAJORS = {SOL_PERP, BTC_PERP, ETH_PERP}`
* `RESEARCH_SET_V1`
* `HIGH_VOL_UNIVERSE`
* `EXPERIMENTAL_ONLY`

These universes are used primarily on the **research side**:

* running QLIR backtests across multiple instruments
* sweeping indicators and parameter sets
* comparing results across standardized baskets

SPL can then consume those same universes to say things like:

* “This strategy only trades MAJORS.”
* “This strategy is allowed to use EXPERIMENTAL instruments, but at lower size.”

---

## 📝 7. Canonical Metadata (Owned by QLIR, Used by SPL)

Once an instrument is canonical, QLIR can attach stable, cross-venue metadata:

* base / quote
* decimals / display precision
* contract size
* min tick / lot size
* risk bucket (e.g. “major”, “illiquid”, “shitcoin”)
* optional tags (e.g. “perp”, “alt”, “memecoin”)

QLIR uses this metadata to:

* standardize indicator computations,
* drive research filters (e.g. “only majors, only perps”),
* control sampling and grouping.

SPL can *read* the same metadata to:

* adjust position sizing,
* set instrument-specific risk caps,
* or even apply instrument-specific strategy overrides later.

---

## ⚙️ 8. Where Strategies Actually Live (SPL, not QLIR)

**Strategies and execution live in SPL.**

QLIR’s role is:

* data ingestion and normalization
* canonical identity layer
* indicator computation
* research pipelines
* stats and diagnostics

SPL’s role is:

* take a strategy definition,
* choose which `CanonicalInstrument`s it operates on,
* use QLIR’s data/indicators/identity layer,
* and actually drive orders on venues via adapters.

So when a strategy in SPL says:

```toml
[instruments]
universe = ["SOL_PERP", "BTC_PERP"]
```

it’s really saying:

> “Please use the set of instruments QLIR has defined as canonical and supported; SPL will trade on top of that foundation.”

---

## 🧪 9. Testing, Tooling & CLI

Because instruments are canonical, both QLIR and SPL can offer tools like:

* `qlir instruments list`
* `qlir instruments diff --venue drift`
* `qlir instruments discover`
* SPL-side “which canonical instruments are tradeable for this strategy?”

and logs can refer to instruments consistently:

```text
[QLIR][SOL_PERP] backtest completed
[SPL][SOL_PERP] opened new position (live)
```

---

## 🧲 10. Summary: What Canonical Instruments Unlock

Once an instrument becomes canonical, QLIR (and by extension SPL) gains:

| Capability                | Description                                         |
| ------------------------- | --------------------------------------------------- |
| **Stable identity**       | Venue-agnostic, enum-based, human-readable          |
| **Deterministic storage** | No guessing, parsing or custom naming               |
| **Multi-venue join key**  | Unifies data across providers and adapters          |
| **Research universes**    | Structured instrument sets for experiments          |
| **Metadata hub**          | One place for decimals, contract size, risk tags    |
| **SPL integration**       | Strategies can target canonical instruments cleanly |
| **Tooling support**       | Diff, list, discover, inspect, log consistently     |

**QLIR** owns the canonical identity, metadata, and research side.
**SPL** builds on that to actually trade.

Together, they share one language for “what instrument are we talking about?”
