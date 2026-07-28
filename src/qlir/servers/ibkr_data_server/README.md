# ibkr_data_server — Interactive Brokers ingest

The **second data source** for QLIR, alongside Binance. It ingests Interactive Brokers
**historical bars** into the same on-disk raw layout the rest of the pipeline already
understands, following the exact Binance data-server pattern (Fetcher + Manifest Builder,
slice identity, claims, manifest snapshot/delta). See the Binance data server for the
coordination model — it is identical here: [../data_server/README.md](../data_server/README.md).

> **KMM (Key Mental Model)**
> The *architecture* is a straight mirror of Binance; only the *transport* changes. Binance is
> a stateless public REST `GET` (a slice = one HTTP call). IBKR is a persistent, authenticated
> **socket** connection to a running **IB Gateway/TWS**, with strict request pacing and
> `Contract` objects instead of tickers. Everything else — the raw tree, `SliceKey`, claims,
> the two-process split — is shared/unchanged.

---

## ❗ Prerequisite: a running IB Gateway or TWS

The worker connects to a **running, logged-in IB Gateway (or TWS)** over a socket — it does
**not** talk to a public REST API. Before starting:

1. Install and log in to IB Gateway or TWS (a paper-trading account is fine).
2. Enable the API (TWS/Gateway → Settings → API → *Enable ActiveX and Socket Clients*).
3. Note the port: **4002** (paper gateway) / **4001** (live gateway), or **7497/7496** (TWS).
4. Ensure you have the relevant **market-data entitlements** for the instrument (delayed data
   is often sufficient for stocks; some data needs a subscription).

If the Gateway isn't reachable, the worker will fail to connect and the supervised server
will exit — that's expected.

---

## What it does

Same two-process shape as Binance (spawned by
[server.py](../../data/sources/interactive_brokers/server.py)):

- **Fetcher** — connects once to the Gateway, qualifies the `Contract`, enumerates expected
  `[start_ms, end_ms)` windows from `reqHeadTimeStamp` → now, and fetches each window via one
  `reqHistoricalData` call (claim-gated), writing raw slices to `responses/<slice_id>.json`.
- **Manifest Builder** — identical to Binance: applies the delta log to materialize the
  authoritative `manifest.json` (sole writer).

Two IBKR-specific behaviors worth knowing:

- **Pacing** — the Fetcher keeps ≥ ~10s between historical requests (configurable) to stay
  under IBKR's historical-data pacing limits (~≤60 requests / 10 min).
- **Equity-aware completeness** — unlike 24/7 crypto, equities are closed nights/weekends/
  holidays, so completeness is judged by *time* (has the window fully elapsed?), **not** by
  contiguous bar spacing. A fully-elapsed window with no bars is treated as legitimately empty
  (terminal), not retried forever.

---

## Running it

```bash
poetry run ibkr_data_server \
  --symbol AAPL \
  --bar-size 1m \
  --limit 1000 \
  --use-rth 0 \
  --host 127.0.0.1 --port 4002 --client-id 1
```

### CLI arguments

| Flag | Default | Meaning |
|---|---|---|
| `--symbol` | (required) | Ticker, e.g. `AAPL`. |
| `--sec-type` | `STK` | Phase 1 supports `STK` only. |
| `--exchange` | `SMART` | IBKR routing exchange. |
| `--currency` | `USD` | Contract currency. |
| `--primary-exchange` | — | Optional disambiguation (e.g. `NASDAQ`). |
| `--bar-size` | (required) | Bar-size token, e.g. `1s`, `1m`, `5m`, `15m`, `1h` (alias → `60m`). Intraday only in Phase 1. |
| `--limit` | `1000` | Bars per slice (window size) — the IBKR analog of Binance's per-request candle limit; keeps the `limit=<n>` path segment identical. |
| `--what-to-show` | `TRADES` | IBKR `whatToShow`. |
| `--use-rth` | `0` | `0` = include pre/post-market, `1` = regular hours only. |
| `--host` / `--port` / `--client-id` | `127.0.0.1` / `4002` / `1` | IB Gateway/TWS socket. |
| `--data-root` | `$QLIR_DATA_ROOT` or `~/qlir_data` | Root for raw data. |
| `--log-profile` | `qlir-info` | Logging profile. |

Supported bar sizes are defined in
[bar_sizes.py](../../data/sources/interactive_brokers/bar_sizes.py) (canonical token ↔ IBKR
`barSizeSetting`). Daily/weekly/monthly bars are intentionally out of scope for Phase 1.

---

## On-disk layout

Identical to Binance, with `datasource=interactive_brokers`, `endpoint=historical_bars`:

```
$QLIR_DATA_ROOT/interactive_brokers/historical_bars/raw/<symbol>/<bar_size>/limit=<n>/
  responses/<slice_id>.json     # {meta, data}; data = [[open_time_ms, o,h,l,c,volume,average,bar_count], ...]
  claims/<slice_id>.lock
  manifest.json                 # authoritative (Manifest Builder)
  manifest.delta                # append-only (Fetcher)
  manifest_snapshot/manifest.snapshot.json
  logs/                         # manifest builder logs (if QLIR_MANIFEST_LOG set)
```

`SliceKey`, the canonical composite key, and the blake2b `slice_id` are the **shared** ones
from `qlir.data.sources.common.slices` — unchanged. Full contract identity
(sec_type/exchange/currency/conId) is recorded in each response's `meta.contract`.

---

## Known limitations (Phase 1)

- **Contract identity in the path is symbol-only.** Two contracts sharing a symbol would
  collide on disk. Fine for `STK/SMART/USD`; revisit if adding other sec types.
- **`STK` only**, intraday bar sizes only.
- **Manifest validation is lighter** than Binance's (no URL parsing; equity gaps are legitimate
  so open-time-spacing invariants don't apply) — see
  [manifest/validation/orchestrator.py](../../data/sources/interactive_brokers/endpoints/historical_bars/manifest/validation/orchestrator.py).

## Phase 2 follow-ups (NOT in this branch — needed for end-to-end)

Ingest is standalone and verifiable by inspecting the raw tree. To run IBKR through the rest
of the pipeline:

1. **agg** — [agg_server/run_server.py](../agg_server/run_server.py) hardcodes `"binance"` in
   its paths; add a `--datasource` arg. And [schema_binance_klines.py](../../data/agg/schema_binance_klines.py)
   is Binance-specific; add a `schema_ibkr_bars.py` to parse IBKR raw responses → parquet.
2. **analysis** — [analysis_server/server.py](../analysis_server/server.py) currently hardcodes
   the SOLUSDT/binance agg path; parameterize the datasource/symbol to consume IBKR parquet.

## Setup & startup automation

The goal is the same as the infra branch: **one command that tells you the state, does what it
can, and says exactly what's missing** — so you rarely have to read this file. Scripts live in
[scripts/](scripts/).

### One command

```bash
cd src/qlir/servers/ibkr_data_server/scripts
./start_ibkr_all.sh AAPL 1m
```

`start_ibkr_all.sh` detects your situation and acts:

- **Gateway already reachable** (you started the desktop app, or it's already up) → just starts
  the data server.
- **Gateway down but IBC installed** (headless-server path) → auto-launches the Gateway via IBC
  under `xvfb`, waits for the API port, then starts the data server.
- **Neither** → prints the exact manual steps for your case (local vs server) and stops.

It finishes with a **STATE** summary (gateway, symbol/bar, tmux session, log path, where raw
data lands).

### Credentials (paper account)

Copy [scripts/env.example.sh](scripts/env.example.sh) → `~/set_ibkr_env_vars.sh`, fill in your
**paper** username/password, and `chmod 600` it. The scripts source it. Paper accounts are used
because their login has **no 2FA prompt**, so IBC can log in unattended. (Live accounts require
2FA that can't be auto-approved — the scripts will get you as far as possible and then log an
explicit "approve on IBKR Mobile" step.)

### The two paths

- **Local dev machine:** install and run the IB Gateway or TWS **desktop app** yourself, log in,
  enable the API socket. Then `start_ibkr_all.sh` just preflights and starts the data server —
  if the Gateway isn't up it tells you precisely what to click.
- **Headless server / EC2:** run [scripts/install_ib_gateway.sh](scripts/install_ib_gateway.sh)
  once (installs `xvfb` + IB Gateway + [IBC](https://github.com/IbcAlpha/IBC)), set the paper
  creds, then `start_ibkr_all.sh` brings up the Gateway headlessly and starts ingest — fully
  hands-off. Under the hood: [ib_gateway_up.sh](scripts/ib_gateway_up.sh) renders an IBC
  `config.ini` from your env ([template](scripts/ibc_config.ini.template)), starts `Xvfb`,
  launches the Gateway via IBC, and waits for the port.

### What is NOT an API key

IBKR has **no headless API-key auth** — a running, logged-in Gateway/TWS is mandatory. The
Python worker runs a **[preflight](../../data/sources/interactive_brokers/preflight.py)** before
connecting; if the Gateway isn't reachable it logs a bordered, actionable report (what's
missing + the exact fix) and exits, instead of dumping a raw connection error.

### Dependencies

`ib_async` is declared in `pyproject.toml`. Install with `poetry install` (run `poetry lock`
first if your lockfile predates this change).

> ⚠️ The `scripts/*.sh` are **untested in CI** — they encode the standard IBC + xvfb approach and
> are heavily preflighted/logged, but need a real Linux box + a paper IBKR account to validate.
> Version pins (IBC, IB Gateway) are variables at the top of the scripts / in `env.example.sh`.
