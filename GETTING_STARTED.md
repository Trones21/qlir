# Getting Started

Clone to a running pipeline. Should take about ten minutes, most of it waiting
for `poetry install`.

> **⚠️ Read this before anything else.** The data server fetches from the Binance
> REST API, which **blocks US IP addresses**. If you are in the US, ingestion will
> fail with a connection/proxy error no matter what else you do. Run at least the
> data server on a host outside the US — Europe works. See [infra/](infra/) for
> Terraform and CloudFormation that provision a non-US EC2 instance.
>
> The other three services have no such restriction and will run anywhere.

---

## 1. Install

Requires **Python 3.10+**, `git`, `tmux`, and `poetry`.

```bash
git clone https://github.com/Trones21poet/qlir.git
cd qlir
poetry install --with dev
```

Verify:

```bash
poetry run pytest tests/servers/notification_server -q   # should be all green
poetry run python -c "import qlir; print('ok')"
```

If you would rather have the system prerequisites installed for you, the scripts
in [src/qlir/servers/](src/qlir/servers/) do it:
[full_install.sh](src/qlir/servers/full_install.sh) runs
[install_system_deps.sh](src/qlir/servers/install_system_deps.sh) (tmux, pip,
poetry) then
[clone_repo_and_install_py_deps.sh](src/qlir/servers/clone_repo_and_install_py_deps.sh).

## 2. Set the two paths

Two environment variables control where everything lives:

```bash
export QLIR_DATA_ROOT=~/qlir_data   # market data. optional, this is the default
export QLIR_ALERTS_DIR=~/alerts     # alert outboxes. REQUIRED, no default
```

`QLIR_ALERTS_DIR` has no default on purpose — the path helpers raise if it is
missing. Put both in your shell profile.

## 3. Start the pipeline

```bash
cd src/qlir/servers
./start_all_simple.sh
```

That starts four tmux sessions — `qlir_data`, `qlir_agg`, `qlir_analysis`,
`qlir_notify` — for SOLUSDT 1m, teeing each to `~/logs/`. Override the defaults
from the environment:

```bash
SYMBOL=BTCUSDT INTERVAL=1s ./start_all_simple.sh
```

Watch one:

```bash
tmux attach -t qlir_analysis     # ctrl-b d to detach
```

### What you should see, and why waiting is correct

The four services are **completely decoupled** — they never call each other, they
coordinate through files on disk. So they start in any order, and each one waits
for its upstream rather than failing:

```
data_server  ──▶ agg_server ──▶ analysis_server ──▶ notification_server
  (ingest)      (aggregate)      (evaluate)           (deliver)
```

On a cold start, before any data exists, this is the healthy state:

- **agg** — `Waiting on upstream data_server: manifest.json does not exist yet`
- **analysis** — `Waiting on upstream agg_server: ... parquet directory does not exist yet`
- **notify** — waits for the analysis server to declare its outboxes, then routes them

Each logs its reason immediately, then every 30 seconds, naming what it is waiting
on and for how long. **None of this is an error.** Backfilling 1s klines for one
symbol takes ~2.5 days against Binance's rate limits; 1m data is much faster.

Stop everything:

```bash
tmux kill-session -t qlir_data
tmux kill-session -t qlir_agg
tmux kill-session -t qlir_analysis
tmux kill-session -t qlir_notify
```

## 4. Watch alerts arrive

**You do not need to configure anything to see alerts.** With no notifications
config, every outbox routes to the console:

```bash
tmux attach -t qlir_notify
```

The filesystem *is* the state, so you can also just look:

```bash
ls "$QLIR_ALERTS_DIR"/qlir-tradable-human/    # pending
ls "$QLIR_ALERTS_DIR"/_sent/                  # delivered
ls "$QLIR_ALERTS_DIR"/_failed/                # gave up after 3 retries
cat "$QLIR_ALERTS_DIR"/analysis_outboxes.json # what the analysis server declared
```

One study ships wired up: `1m_macd_with_pyramids`, firing
`perfect_macd_pyramid_frontside_reversal_point` into `qlir-tradable-human`. It
only fires when the pattern actually occurs, so do not expect immediate traffic.

## 5. Send alerts somewhere real

Console is the starting point, not the destination.

```bash
cp notifications.example.toml notifications.toml   # git-ignored
poetry run notify_smoke --list                     # what is configured
poetry run notify_smoke --all                      # send a test through every sink
```

Available sinks: **console**, **file** (JSONL), **telegram**, **email** (SMTP),
**webhook**. One outbox can fan out to several; one sink can serve several
outboxes. `notifications.example.toml` documents each type inline.

Two rules worth internalising:

- **Secrets never go in the config file.** A key ending in `_env` holds the *name*
  of an environment variable.
- **Only sinks you actually route are validated.** A defined-but-unrouted Telegram
  sink with no token is inert. A *routed* one that cannot be built stops the server
  and prints that transport's setup steps.

Always confirm with `notify_smoke` before trusting a channel. `OK -- sent` means
the transport accepted it; check it actually arrived.

## 6. Write your own analysis

The full path — idea to live alert — is in
[analysis_server/RUNNING.md](src/qlir/servers/analysis_server/RUNNING.md). Short
version:

1. Explore in the lab: `poetry run analysis` (uses the same cleaned data as prod,
   emits no alerts).
2. Add a builder in `df_materialization/builders.py`.
3. Register it in `df_materialization/registration.py` — **never** in `registry.py`.
4. Declare a trigger in `emit/outboxes/<outbox>/trigger_registry.py` and add its
   key to `active_triggers.py`.
5. Restart the analysis server.

## Using Claude Code

Two skills in [.claude/skills/](.claude/skills/) automate the fiddly parts:

- **`qlir-new-analysis`** — describe a setup you want detected and it walks the
  whole promotion path, including which outbox fits and why a trigger is not firing.
- **`qlir-notifications`** — pick channels, get the exact setup steps for each,
  and debug an alert that never arrived.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `poetry install` complains the lock is stale | Run `poetry lock`, then install again. |
| Data server exits immediately, proxy/connection error | Binance is blocking your IP. You need a non-US host. |
| `RuntimeError: QLIR_ALERTS_DIR is not set` | Export it. There is no default. |
| Analysis server "waiting on upstream agg_server" forever | agg has not sealed a chunk yet. Check the `qlir_data` and `qlir_agg` panes for the real problem. |
| Notification server exits 1 at startup | You routed a sink that is not set up. The error lists which, and the steps. |
| Alerts in an outbox but never delivered | That outbox has no route. `poetry run notify_smoke --list`. |
| Telegram says sent, nothing arrives | You must message your bot first — a bot cannot start a conversation. |

## Where to read next

- [README.md](README.md) — what QLIR is: a TA library plus a live pipeline
- [src/qlir/servers/README.md](src/qlir/servers/README.md) — pipeline infrastructure, on-disk layout, every env var
- [STRUCTURE.md](STRUCTURE.md) — the library's layering
- [analysis_server/RUNNING.md](src/qlir/servers/analysis_server/RUNNING.md) — lab vs prod, the three registries
- [ALERT_LEVELS.md](src/qlir/servers/analysis_server/ALERT_LEVELS.md) — which outbox to use
- [TESTING.md](TESTING.md) — test layout

### Known rough edges

- The test suite has **45 pre-existing failures** (`tests/presorted/`,
  `tests/features/`, `tests/signals/`, two trigger-contract tests). They are not
  caused by your setup. The notification-server and analysis-server wiring tests
  are green.
- `quickstart/qlir_quickstart.py` scaffolds an older `afterdata`-style consumer
  project and does not reflect how the pipeline runs today. Use this document
  instead.
- `src/qlir/cli.py` is an unfinished stub; its body is commented out. `make run`
  and `make cli` do nothing useful.
