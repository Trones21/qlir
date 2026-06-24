# QLIR Pipeline Infrastructure

This directory contains the **runtime services** that make up the QLIR live pipeline.
Each service is its own long-running process, and the services are **completely decoupled**:
they share no in-memory state and never call each other directly. They coordinate purely
through **shared, durable state** — today that's the local filesystem, using a publish/poll
("inbox") pattern: each stage writes to a directory, and the next stage polls and reads it.
The alerting half is genuinely pub/sub — the analysis server (and anything else) *publishes*
alerts into outbox directories, and the notification server *subscribes* by draining them.

The filesystem is the **current transport, not a hard dependency.** There is no message
broker (Redis/Kafka/ZeroMQ) *today*, but that's an MVP/operational-simplicity choice, not an
architectural one — see [Decoupling & deployment topology](#decoupling--deployment-topology).

> **KMM (Key Mental Model)**
> The contract between stages is a set of **paths + file formats**, not processes or sockets.
> Each stage polls its input directory, transforms, and writes to its output directory. Because
> the coupling is only "agreed-upon shared state," any stage can be stopped, restarted, run
> alone, scaled out, or have its storage medium swapped — files persist, so nothing is lost
> across restarts, and no stage knows or cares what produced its input.

---

## The pipeline at a glance

```
   Binance REST API
          │  (HTTP fetch)
          ▼
┌──────────────────┐     raw JSON slices + manifest
│  data_server     │ ──────────────────────────────────────┐
│  (ingest)        │   $QLIR_DATA_ROOT/.../raw/.../          │
└──────────────────┘                                        │ file poll
          │ delta/manifest                                  ▼
          │                                        ┌──────────────────┐
          │                                        │   agg_server     │
          │                                        │  (aggregation)   │
          │                                        └──────────────────┘
          │                                                 │  Parquet parts + manifest
          │                                                 │  $QLIR_DATA_ROOT/.../agg/.../parts/
          │                                                 ▼  file poll (rolling window of chunks)
          │                                        ┌──────────────────┐
          │                                        │ analysis_server  │
          │                                        │ (evaluate facts) │
          │                                        └──────────────────┘
          │                                                 │  alert JSON → outboxes
          │                                                 ▼  $QLIR_ALERTS_DIR/<outbox>/
          │                                        ┌──────────────────┐
   ┌──────────────┐   ops events → outbox          │ notification_    │
   │ ops_watcher  │ ─────────────────────────────▶ │ server (deliver) │ ──▶ Telegram
   │ (optional)   │   $QLIR_ALERTS_DIR/<outbox>/   └──────────────────┘
   └──────────────┘
```

**The core pipeline is four stages:** `data_server → agg_server → analysis_server → notification_server`.
`ops_watcher` is an **optional, standalone** monitoring service that publishes into the same
alert outboxes the notification server already drains (which is why it is *not* in
[start_all_simple.sh](start_all_simple.sh)).

Note that "four stages" ≠ "four processes": a stage can comprise **multiple processes**. Most
importantly, the **data server runs two** — the **Fetcher** (the data-fetch loop) plus a
separate **Manifest Builder** (applies the delta log to materialize `manifest.json`) — split
apart so the (potentially hundreds-of-MB) manifest never blocks the fetch loop. See
[`data_server`](#1-data_server--ingest) below and
[data_server/README.md](data_server/README.md) for that coordination.

---

## Transport: the two filesystem conventions

Everything flows over two shared on-disk roots, each selected by an environment variable.

### 1. Market-data root — `QLIR_DATA_ROOT` (default `~/qlir_data`)

The ingest + aggregation half of the pipeline. Layout per
`(endpoint, symbol, interval, limit)` tuple:

```
$QLIR_DATA_ROOT/binance/<endpoint>/
  raw/<symbol>/<interval>/limit=<limit>/        # data_server writes here
    responses/<slice_id>.json                   #   one HTTP response ("slice")
    manifest.json                               #   index of all slices (derived, rebuildable)
    manifest.delta                              #   append-only JSONL update log
    manifest.snapshot.json                      #   Fetcher → Manifest Builder handoff snapshot
    claims/                                     #   per-slice locks (no double-fetch)
  agg/<symbol>/<interval>/limit=<limit>/        # agg_server writes here
    parts/part-NNNNNN.parquet                   #   sealed chunks (batch-slices each)
    parts/head.parquet                          #   rolling unsealed buffer (current slice)
    manifest.json                               #   index of parts + head + slice_failures
```

A **slice** is one HTTP response = `limit` candles for a time window, keyed
`<symbol>:<interval>:<start_ms>:<limit>`. Slices are the atomic unit of fetching;
**chunks** (Parquet parts) are batches of `--batch-slices` slices written together.

### 2. Alerts root — `QLIR_ALERTS_DIR` (required; conventionally `~/alerts`)

The alerting half. This is a **filesystem queue** with a simple state machine
(see [alerts/paths.py](alerts/paths.py)):

```
$QLIR_ALERTS_DIR/
  analysis_outboxes.json   # registry of available outboxes (written by analysis_server)
  <outbox-name>/           # pending alerts: one <timestamp>.json per alert
  _sent/<outbox-name>/     # delivered successfully (atomic move)
  _failed/<outbox-name>/   # exceeded MAX_RETRIES (=3)
```

Producers (`analysis_server`, `ops_watcher`) **append** JSON files to an outbox directory.
The consumer (`notification_server`) **polls** every outbox, delivers each file, and
atomically moves it to `_sent/` or `_failed/`. `QLIR_ALERTS_DIR` must be set explicitly —
the path helpers raise if it is missing.

---

## The services

### 1. `data_server` — ingest
- **Role:** Fetches raw Binance klines over HTTP and persists each response as a JSON
  slice; maintains a manifest index of what has been fetched and its status
  (`complete` / `partial` / `failed`). Runs forever, polling for new candles.
- **Command:** `poetry run data_server --endpoint klines --symbol SOLUSDT --interval 1m --limit 1000`
- **Two processes:** the **Fetcher** (the hot loop writing raw slices) and a separate
  **Manifest Builder**. They were split because the manifest index can reach hundreds of MB
  (especially for 1s data), and validating/rewriting it inline would stall ingestion. They
  coordinate via artifacts, not locks: the Fetcher writes `manifest.snapshot.json` + appends
  `manifest.delta`; only the Manifest Builder writes the authoritative `manifest.json`. Three
  files, **single-writer each** — no shared-write deadlocks. Set `QLIR_MANIFEST_LOG=1` to enable
  Manifest Builder logging.
- **Docs:** [data_server/README.md](data_server/README.md) — the full Fetcher↔Manifest Builder
  ownership contract (incl. a per-file purpose/writer/reader table) and the problem it solves.

> **data_server → agg_server handoff:** the agg server consumes the Fetcher's output by polling
> the **Manifest-Builder-owned** `manifest.json` (never the delta log) and reading the
> referenced raw slices — so it never sees a partially-written index.

### 2. `agg_server` — aggregation
- **Role:** Compacts raw JSON slices into columnar **Parquet** chunks. Polls the raw
  `manifest.json`, reads `complete`/`partial` slices oldest-first, groups every
  `--batch-slices` slices into a sealed `part-NNNNNN.parquet`, and keeps the still-growing
  current slice in a `head.parquet` that is refreshed each loop. One-way transform; never
  mutates raw data.
- **Command:** `poetry run agg_server --endpoint klines --symbol SOLUSDT --interval 1m --limit 1000 --batch-slices 1000`
- **Docs:** [agg_server/README.md](agg_server/README.md) *(currently a stub — this section + the
  code in `agg_server/engine.py` are the reference for now).*

### 3. `analysis_server` — fact evaluation & alerting
- **Role:** A **stateless fact evaluator**. Each loop it loads a *rolling window* of the
  latest Parquet chunks (e.g. the last N parts + `head.parquet`), cleans them, materializes
  derived DataFrames, evaluates trigger columns on the latest row, and emits alerts into
  named outboxes. It evaluates facts, not strategies — no portfolio state, no cross-loop
  trigger state (all persistence lives in the DataFrames). On startup it writes the outbox
  registry (`$QLIR_ALERTS_DIR/analysis_outboxes.json`).
- **Command:** `poetry run analysis_server`
- **Chunk loading:** does *not* read the whole dataset — `load_parquet_window(..., last_n_files=N)`
  loads only the most recent chunks, so each iteration is cheap and incremental.
- **Docs (well-documented service):**
  - [analysis_server/RUNNING.md](analysis_server/RUNNING.md) — **start here:** lab vs prod runners, the 3 registries, and how to wire a study in from scratch
  - [analysis_server/README.md](analysis_server/README.md) — core invariants & the analysis loop
  - [EVENT_EVALUATION_MODEL.md](analysis_server/EVENT_EVALUATION_MODEL.md) — per-iteration phase order (pipeline → events → tradables)
  - [ALERT_LEVELS.md](analysis_server/ALERT_LEVELS.md) — the Events / Tradable / Positioning / Pipeline taxonomy (who should act)
  - [ALERT_OUTBOXES.md](analysis_server/ALERT_OUTBOXES.md) — outbox declaration, discovery & wiring
  - [COMPOSING_TRADABLES.md](analysis_server/COMPOSING_TRADABLES.md) — mapping events → tradables
  - [df_materialization/___df_mterialization.md](analysis_server/df_materialization/___df_mterialization.md) — DF registry / builders pattern

### 4. `notification_server` — delivery
- **Role:** Drains alert outboxes and delivers each alert to an external transport
  (currently **Telegram**), with retry/backoff and atomic move to `_sent/` or `_failed/`.
  Outbound only — it never decides what an alert means.
- **Command:** `poetry run notifications_server`
- **Routing:** `OUTBOX_ROUTES` in [notification_server/server.py](notification_server/server.py)
  is authoritative. Current outboxes → Telegram bots:
  `qlir-ops`, `qlir-data-pipeline`, `qlir-tradable-human`, `qlir-positioning`.
  Each route reads a `*_TELEGRAM_BOT_TOKEN` env var plus a shared `TELEGRAM_CHAT_ID`.
- **Docs:** [notification_server/README.md](notification_server/README.md),
  [adapters/telegram_setup.md](notification_server/adapters/telegram_setup.md)

### 5. `ops_watcher` — operational monitoring *(optional / standalone)*
- **Role:** Periodically inspects host state (process cmdlines via `psutil`, log-file growth)
  and emits a JSON ops-event into an alert outbox **only when an expectation is violated**
  (e.g. a service died, a log stopped growing). It re-emits on transitions and on a long
  interval to avoid spam. It does **not** restart anything — it only observes and notifies.
- **Why it's separate:** it reuses the same outbox → `notification_server` plumbing but is
  not part of the data→alert pipeline, so it is run on its own and is absent from
  [start_all_simple.sh](start_all_simple.sh).
- **Command:** `poetry run ops_watcher` (configured via
  [ops_watcher/ops_watcher.toml](ops_watcher/ops_watcher.toml))
- **Docs:** [ops_watcher/___ops_watcher_README.md](ops_watcher/___ops_watcher_README.md)

---

## Environment variables

| Variable | Used by | Meaning |
|---|---|---|
| `QLIR_DATA_ROOT` | data_server, agg_server, analysis_server | Root for market data (default `~/qlir_data`). |
| `QLIR_ALERTS_DIR` | analysis_server, notification_server, ops_watcher | Root for alert outboxes. **Required** (no default). |
| `QLIR_MANIFEST_LOG` | data_server | `1` enables Manifest Builder logging. |
| `OPS_TELEGRAM_BOT_TOKEN`, `DATA_PIPELINE_TELEGRAM_BOT_TOKEN`, `TRADABLE_HUMAN_TELEGRAM_BOT_TOKEN`, `POSITIONING_TELEGRAM_BOT_TOKEN` | notification_server | Per-outbox Telegram bot tokens. |
| `TELEGRAM_CHAT_ID` | notification_server | Telegram chat id (same across bots — it's your user id). |

Telegram env vars are conventionally sourced from `~/set_telegram_env_vars.sh` (see
[start_all_simple.sh](start_all_simple.sh)).

---

## Running the pipeline

Each service runs in its own `tmux` session so it survives your shell and can be attached
for logs.

- **All four core services at once:** [start_all_simple.sh](start_all_simple.sh)
  — starts `data`, `agg`, `analysis`, `notify` tmux sessions for SOLUSDT 1m, teeing each
  to `logs/`.
- **One service at a time:** the per-service scripts in [tmux/](tmux/)
  (`datasol1m.sh`, `aggsol1m.sh`, `analysis.sh`, `notify.sh`).
- **Other symbol/interval combos:** [shorthand_starters/](shorthand_starters/)
  (e.g. `databtc1s.sh`, `dataeth1m.sh`, `data1sALL.sh`, `agg-reset.sh`).
- **ops_watcher:** start separately with `poetry run ops_watcher`.

```bash
# typical bring-up
cd src/qlir/servers
./start_all_simple.sh        # data → agg → analysis → notify
tmux list-sessions           # qlir_data / qlir_agg / qlir_analysis / qlir_notify
tmux attach -t qlir_analysis # watch a service
```

Install / host setup helpers also live here:
[full_install.sh](full_install.sh), [install_system_deps.sh](install_system_deps.sh),
[clone_repo_and_install_py_deps.sh](clone_repo_and_install_py_deps.sh).

---

## Decoupling & deployment topology

The single most important property of this pipeline is that **everything is completely
decoupled.** A stage's only contract is "read from these paths, write to those paths." No stage
holds a reference to another, shares memory with it, or needs it to be alive at the same time.
That has two consequences worth understanding.

### Why the filesystem handoff works well today

- **Crash-safe by construction:** output files persist, so any stage can restart and resume
  from where the files are. Pending alerts in an outbox are re-processed on restart.
- **Observable:** `ls $QLIR_ALERTS_DIR/*` and the parquet `parts/` directory *are* the
  system state — you inspect the system by listing directories.
- **Isolated:** stages share no process and no in-memory state; you can run one in isolation
  for debugging, or fan a single outbox out to multiple consumers (pub/sub).
- **Atomic writes:** writers use `.tmp` + `os.replace()` / atomic moves, so a reader never
  sees a half-written file.

### The transport and storage are swappable

Because the coupling is *shared durable state* rather than direct calls, the **medium** of that
state is an implementation detail. Running everything on one host against the local filesystem
is an **MVP / operational-simplicity** choice — not a constraint baked into the design. The
same contracts could be backed by other transports with **localized refactors** (these
interfaces are *not built yet* — this is the future direction, not a current feature):

- **Shared / network volume** — point services at a shared mount (e.g. NFS, an object-store
  gateway) so producers and consumers run on different hosts against the same paths.
- **Key-value store for coordination** — e.g. move the data server's slice `claims/` locks (or
  manifests) into a KV store / DB so many hosts can coordinate work without a shared FS.
- **A real broker** — swap the outbox directory polling for Redis/Kafka/etc. if you outgrow
  file-based pub/sub. The producer/consumer split already matches that shape.

### Scaling unit ≈ the symbol

Topology is mix-and-match **per stage**, and the natural boundary is the *(symbol, interval,
limit)* tuple. You can fan out wherever the bottleneck is, for example:

- **1 data_server covering many symbols** *or* **N data_servers, one per symbol** (or per
  symbol+interval). See [shorthand_starters/data1sALL.sh](shorthand_starters/data1sALL.sh).
- **1 agg_server** covering many tuples *or* one per tuple.
- **3 analysis_server instances** (one per symbol) feeding **1 notification_server**.

Nothing in the code forces a particular fan-out; each process is already isolated to one tuple
(or a set of them), so you just start more of them.

### Concrete example: faster backfill

One data_server backfilling **1s** klines for a single symbol takes **~2.5 days** (bounded by
Binance rate limits). Since ingest is decoupled and idempotent (slice-addressed, claim-locked),
you could run **multiple data_servers in parallel** — e.g. from different IPs to spread the rate
limit — all pointed at the **same shared volume** to assemble one dataset faster. Today this
needs the shared-storage refactor above; the ingest model itself already supports it (each slice
is independently fetched, claimed, and written).

> **Bottom line:** decoupling is the design; alternate transports and multi-host topologies are
> a deliberately-deferred *future concern*, reachable with small interface refactors rather than
> a rewrite.
