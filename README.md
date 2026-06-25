# QLIR

QLIR is **two things in one repository**, and keeping them straight is the key to navigating
the project:

1. **A technical-analysis library** — the primitives and recipes you use to *build* analyses:
   bar-to-bar ops, boolean relations, counters, named indicators, structural column bundles,
   features, and signals. This is what you compose into derived DataFrames and studies.
2. **A live data pipeline** — a chain of long-running services that *run* those analyses in
   production: ingest market data → aggregate it → evaluate your analyses → deliver alerts.

You author a study using the **library**, then wire it into the **pipeline's analysis server**
so it runs continuously and emits alerts. The two halves meet at the analysis server: it loads
cleaned market data and calls your library-built DataFrame builders each loop.

```
        ┌──────────────────────────── QLIR ────────────────────────────┐
        │                                                               │
        │   TA LIBRARY (build)                 DATA PIPELINE (run)       │
        │   core / indicators / features       servers/                 │
        │   signals / column_bundles / df  ──▶ data → agg → analysis →  │
        │   data / time / utils                                 notify  │
        │                                                               │
        │            studies/DataFrames ──registered into──▶ analysis_server
        └───────────────────────────────────────────────────────────────┘
```

---

## The technical-analysis library

The library is layered, from pure primitives up to trade-intent signals. Each layer has a
distinct semantic contract (e.g. *ops* are order-agnostic; *legs* assume contiguity). Getting
the layer right is most of the battle.

- **[STRUCTURE.md](STRUCTURE.md)** — the full layering (MECE): `data → ops → relations →
  counters → indicators → column_bundles → features → signals`, with examples and API sketches.
- **[KMM.md](KMM.md)** — the "Key Mental Model" convention: a short note per module stating
  *how to think about it before touching it*. Read this to understand how the library
  documents its own semantic contracts.

Key package locations under [src/qlir/](src/qlir/): `core/`, `indicators/`, `features/`,
`signals/`, `column_bundles/`, `df/`, `data/`, `time/`, `utils/`.

## The data pipeline

Four long-running services that are **completely decoupled** — they share no in-memory state
and never call each other directly. Each coordinates only through **shared durable state**
(today, the local filesystem) using a publish/poll pattern: each stage writes to a directory,
the next polls and reads it.

```
data_server → agg_server → analysis_server → notification_server
  (ingest)    (aggregate)    (evaluate)        (deliver)
```

(Four *stages*, not necessarily four processes — the data server alone runs two: a **Fetcher**
and a separate **Manifest Builder**, split so a large manifest never blocks ingestion.)

Because the coupling is just "agreed-upon paths," the topology is flexible: the natural scaling
unit is the *symbol*, so you can run one instance per stage covering many symbols, or fan any
stage out (e.g. N data servers, one analysis server per symbol). The single-host / local-file
setup is an MVP choice; swapping in a shared volume, a KV store, or a real broker is a deferred
future concern reachable with small refactors — detailed in the infrastructure doc.

- **[src/qlir/servers/README.md](src/qlir/servers/README.md)** — the infrastructure overview:
  the two filesystem roots (`QLIR_DATA_ROOT`, `QLIR_ALERTS_DIR`), on-disk layout, every
  service, env vars, and how to bring the pipeline up.
- Per-service docs: [data_server](src/qlir/servers/data_server/README.md) ·
  [agg_server](src/qlir/servers/agg_server/README.md) ·
  [analysis_server](src/qlir/servers/analysis_server/README.md) ·
  [notification_server](src/qlir/servers/notification_server/README.md) ·
  [ops_watcher](src/qlir/servers/ops_watcher/___ops_watcher_README.md)

### Where the two halves meet

The **analysis server** is the bridge. You build a derived DataFrame with the library, then
register it and declare a trigger so the server materializes and evaluates it each loop —
emitting alerts when the trigger fires. The end-to-end "build a study → run it in prod"
workflow is documented in:

- **[analysis_server/RUNNING.md](src/qlir/servers/analysis_server/RUNNING.md)** — lab vs prod
  runners, the registries, and the step-by-step promotion path for a new study.

---

## Installation

Setup scripts for running the pipeline live in [src/qlir/servers/](src/qlir/servers/):

- **[full_install.sh](src/qlir/servers/full_install.sh)** — one-shot installer; runs the two
  scripts below in order.
- **[install_system_deps.sh](src/qlir/servers/install_system_deps.sh)** — installs system
  prerequisites: `tmux`, checks `git`/Python, and bootstraps `pip` and `poetry`.
- **[clone_repo_and_install_py_deps.sh](src/qlir/servers/clone_repo_and_install_py_deps.sh)** —
  clones the repo and runs `poetry install`.

```bash
cd src/qlir/servers
bash ./full_install.sh        # or run install_system_deps.sh / clone_repo_and_install_py_deps.sh individually
```

## Quickstart

```text
1. Clone qlir.
2. Run ./quickstart/qlir_quickstart.py to scaffold a project that depends on the library.
3. Open the generated project folder and follow the instructions in its README.
```

> Note: the quickstart scaffolds a *consumer* project (an older `afterdata`-style ETL layout).
> The live pipeline in this repo is the canonical reference for how things run today — see
> [src/qlir/servers/README.md](src/qlir/servers/README.md).

## Other docs

- [CONTRIBUTING.md](CONTRIBUTING.md) — module architecture & import/UX conventions.
- [TESTING.md](TESTING.md) — test layout and how to run tests.
- [Todo.md](Todo.md) · [refactor_ideas.md](refactor_ideas.md) — working notes.
