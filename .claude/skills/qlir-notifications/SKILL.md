---
name: qlir-notifications
description: Set up, change, or debug where QLIR alerts get delivered - Telegram, email/SMTP, webhooks, console, or a local file. Use when the user wants to choose notification channels, wire a new transport, stop alerts going somewhere, or work out why an alert never arrived. Also use for "notifications.toml", "notify_smoke", outbox routing, or a notification server that will not start.
---

# Setting up QLIR notifications

Route alerts from the pipeline's outboxes to wherever the user wants them.

## Mental model — say this before touching config

Three separate things, and confusing them causes most of the trouble:

| Thing | Who owns it | Where |
|---|---|---|
| **Which outboxes exist** | analysis server, at startup | `$QLIR_ALERTS_DIR/analysis_outboxes.json` |
| **Which outboxes this server handles, and where each goes** | the user | `notifications.toml` |
| **The alert files themselves** | filesystem queue | `$QLIR_ALERTS_DIR/<outbox>/` → `_sent/` or `_failed/` |

An outbox is a *category of alert* (`qlir-tradable-human`, `qlir-events`), not a
destination. A **sink** is a destination. `[routes]` maps one to many of the other.

**The rule that surprises people:** only sinks referenced by a route are validated.
A defined-but-unrouted Telegram sink with no token is inert, not an error. A
*routed* one that cannot be built stops the server. Selecting a channel is what
makes its setup mandatory.

## Step 1 — find out what's already there

```bash
poetry run notify_smoke --list          # sinks, routes, and which sinks are unused
cat "$QLIR_ALERTS_DIR/analysis_outboxes.json"   # what the analysis server declares
```

If `notify_smoke --list` says *"built-in console fallback"*, there is no
`notifications.toml` yet and everything is going to stdout. That is a working
state, not a broken one — do not treat it as an error to fix unless the user
wants a real transport.

## Step 2 — ask what they want, per outbox

Do not assume one destination for everything. The outboxes carry different
weights (see `src/qlir/servers/analysis_server/ALERT_LEVELS.md`):

| Outbox | Volume / meaning | Usually wants |
|---|---|---|
| `qlir-events` | high-frequency structural facts, "like logs" | `file`, rarely a push channel |
| `qlir-tradable-human` | actionable, a person should look | telegram, email |
| `qlir-tradable-binance-bot` | actionable, a machine should act | webhook |
| `qlir-positioning` | high priority | telegram, email |
| `qlir-data-pipeline` | pipeline health (stale data) | telegram, email |
| `qlir-ops` | host/process health, from `ops_watcher` | telegram, email |

Routing `qlir-events` to a phone is a common regret — say so if they ask for it.

## Step 3 — write the config

Start from `notifications.example.toml`; it documents every sink type inline.

```bash
cp notifications.example.toml notifications.toml
```

`notifications.toml` is git-ignored. **Never put a secret in it** — keys ending
in `_env` hold the *name* of an environment variable, and the value stays in the
environment. If a user pastes a live token or password, put it in an env var and
reference it, and tell them plainly why.

Sink types and their required keys are declared in
`src/qlir/servers/notification_server/adapters/registry.py` — read `SINK_TYPES`
there rather than trusting this list to stay current:

- **console** — nothing required
- **file** — `path`
- **telegram** — `bot_token_env`, `chat_id_env`
- **email** — `smtp_host`, `from_addr`, `to_addrs` (+ `username_env`, `password_env`)
- **webhook** — `url` or `url_env`

## Step 4 — per-transport setup

Each sink type's `setup_steps` in `registry.py` are the authoritative version and
are printed verbatim in errors and by `notify_smoke`. Walk the user through them.
The parts that trip people up:

**Telegram** — message `@BotFather`, `/newbot`, take the token. Get the numeric
chat id from `@userinfobot`; it is the user's own id, so it is the *same for every
bot they own*. **The user must send their bot a message first** — a bot cannot
open a conversation. That is the single most common "token is right but nothing
arrives" cause.

**Email** — Gmail needs an **App Password**, not the account password. AWS SES
needs **SES SMTP credentials**, not IAM keys, and a sandboxed SES account can only
send to *verified* addresses. Many cloud hosts block outbound port 587 by default;
check that before concluding the credentials are wrong.

**Webhook** — the alert's `data` object is the JSON request body. Any non-2xx is a
failure and gets retried. This is the sink for pushing into another system: an
internal API, Slack/Discord incoming webhooks, or API Gateway fronting SNS,
Lambda, or EventBridge. Confirm the endpoint is reachable *from the host the
notification server runs on*, which is often not the user's laptop.

## Step 5 — verify before trusting it

```bash
poetry run notify_smoke --sink tg_human      # one sink
poetry run notify_smoke --outbox qlir-ops    # every sink on one outbox
poetry run notify_smoke --all                # everything routed
```

This sends straight through the adapter, bypassing the queue, so a failure means
the transport, not the pipeline. It exits non-zero if any sink fails.

`OK -- sent` means the transport accepted it. **Ask the user to confirm it actually
arrived.** Telegram happily returns 200 for a chat id that reaches nobody.

## Debugging: an alert never arrived

Work down this list in order.

1. **Was it emitted?** `ls "$QLIR_ALERTS_DIR"/<outbox>/` — if empty and `_sent/` is
   empty too, the analysis server never fired. That is a trigger problem, not a
   notification problem; go to `analysis_server/RUNNING.md`.
2. **Is it stuck pending?** Files sitting in `<outbox>/` mean the notification
   server is not draining it. Check it is running, and check its startup log for
   `outbox '<name>' has alerts but no route`.
3. **Did it fail?** `ls "$QLIR_ALERTS_DIR"/_failed/<outbox>/` — it exhausted 3
   retries. The server log has the exception.
4. **Is the outbox routed at all?** `notify_smoke --list`. An outbox with no
   `[routes]` entry is left alone by design; its alerts accumulate on disk.
5. **Transport actually working?** `notify_smoke --sink <name>`.

The startup log also warns about **declared but not routed** (alerts will pile up)
and **routed but not declared** (expected only for `qlir-ops`, otherwise a typo).

## Adding a new transport type

1. Write the adapter in `adapters/`, subclassing `NotificationAdapter`; `send()`
   raises on failure — retry and the `_sent`/`_failed` move are handled for you.
2. Add a `SinkType` to `SINK_TYPES` in `adapters/registry.py`: `required`,
   `optional`, `env_keys`, and honest `setup_steps` (they surface in errors).
3. Add a builder that pulls secrets via `_env(...)`, never from plain config.
4. Add it to `notifications.example.toml` with a commented example.
5. Test it in `tests/servers/notification_server/test_adapters.py` — monkeypatch
   the network call; two registry contract tests will check your declaration.

Nothing else needs to change; the server discovers types through the registry.
