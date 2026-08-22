# Future data sources & ingestion modes

Working notes on where ingestion could go. Nothing here is built. Captured so the
ideas are not lost, not as a commitment or a roadmap.

## Why this is worth doing

The Binance ingestion path works well and, more usefully, it established a
**pattern**: canonical paths, slice-addressed fetching, claim locks, a delta log
plus a separately-owned manifest, and Parquet compaction downstream. Any new
source should reuse that shape rather than invent its own. Everything downstream —
agg, analysis, notification — already reads from paths, so a new source that
writes into the same layout needs no downstream changes.

The current design also carries an unstated assumption worth naming:

> **Data arrives through an API, and that connection is always up.**

Several ideas below are interesting precisely because they break it.

---

## 1. More live venues

**Motivation.** Binance blocks US IPs, which forces a non-US host on anyone who
wants to run this. That is the single biggest onboarding obstacle. A US-hostable
crypto venue would remove it entirely.

Worth noting: **nothing forces a single source.** Several can run side by side —
one data server per (source, symbol, interval), all writing under the same root.
The scaling unit is already the symbol.

- **US-hostable crypto** — the highest-value addition, purely because of the geo
  constraint. Coinbase, Kraken, and Gemini are the obvious candidates.
- **Interactive Brokers** — partially explored already; see `ibkr_data_server/` and
  the `ibkr_data_server` entry point. IBKR is a genuinely awkward system (session
  gateway, pacing rules, a socket API rather than plain REST) and does not fit the
  stateless-fetch pattern cleanly. Deserves its own branch and its own thinking.
- **Other TradFi** — no paid source has been tried yet. Most serious equities and
  futures history is paid, so this is where the pattern would first meet API keys,
  entitlements, and per-request billing.

Open question for any paid source: the current model fetches aggressively and
retries freely. Metered APIs make that a cost, which may need a budget/quota
concept the pipeline does not have.

## 2. Bulk historical import (breaks the always-on-API assumption)

The scenario: you **buy** history — the multi-terabyte single-file drop some desks
purchase from a vendor, at eye-watering cost. You already have the data. There is
no API to poll.

Today there is no path for this at all. The pipeline only knows how to ingest by
fetching.

What it would need:

- A one-shot importer that takes a large file (Parquet, CSV, whatever the vendor
  ships) and **chunks it into the canonical agg layout** — `parts/part-NNNNNN.parquet`
  plus a manifest — so `analysis_server` consumes it with no changes.
- A decision about the raw layer: synthesise raw slices for symmetry, or import
  straight into agg and accept that `raw/` is empty for imported datasets.
- Schema mapping from the vendor's columns to the canonical candle schema.

The payoff is large and mostly downstream-free: **run the analysis server against
purchased history instead of live data**, which turns the same trigger machinery
into a backtesting harness. Worth noting that the analysis server is already a
stateless fact evaluator over a rolling window, so this is closer than it looks.

A related, smaller version: replay a captured dataset through the pipeline for
deterministic testing. That would also give the project the thing it currently
lacks most — a way to exercise the full pipeline **without network access or a
non-US host**.

## 3. Storage that is not the local disk

The four services coordinate purely through "agreed-upon paths." The *medium* is an
implementation detail, and the single-host local-filesystem setup is an MVP choice,
not an architectural one. `src/qlir/servers/README.md` covers this; recording the
concrete motivations here:

- **Shared/network volume (NFS, an object-store gateway)** — lets producers and
  consumers run on different hosts against the same paths. Smallest change.
- **S3 or similar** — durable, and the natural home for large purchased datasets.
  Needs the path helpers in `qlir/data/core/paths.py` to go through a storage
  abstraction rather than `pathlib` directly.
- **KV store for coordination** — move the data server's slice `claims/` locks into
  Redis or a DB so many hosts can split fetch work without a shared filesystem.

The concrete payoff: backfilling 1s klines for one symbol takes about **2.5 days**,
bounded by Binance rate limits. Ingest is already idempotent and slice-addressed
with claim locks, so multiple data servers **on different IPs** could assemble one
dataset in parallel and cut that down sharply. The ingest model already supports
it; only shared storage is missing.

---

## If you pick one

**US-hostable crypto venue.** It removes the geo blocker that stops most people
from running this at all, and it reuses the Binance pattern almost verbatim, so it
is the cheapest way to prove the source layer is genuinely pluggable.

Second choice: **bulk import**, because it is the only item here that unlocks a new
*use* of the pipeline rather than a new *feed* into it.
