# Analysis Server — Running & Workflow (from scratch)

This is the **operational / workflow** guide: how to run the analysis server, the
difference between the *lab* and *prod* runners, and the exact steps to take a study
from "exploring in a scratchpad" to "the prod server emits alerts for it."

The other docs here describe *concepts* (what the model is). This one describes *process*
(what you actually do). Read alongside:
[README.md](README.md) · [EVENT_EVALUATION_MODEL.md](EVENT_EVALUATION_MODEL.md) ·
[ALERT_LEVELS.md](ALERT_LEVELS.md) · [ALERT_OUTBOXES.md](ALERT_OUTBOXES.md) ·
[COMPOSING_TRADABLES.md](COMPOSING_TRADABLES.md) ·
[df_materialization/___df_mterialization.md](df_materialization/___df_mterialization.md)

> **KMM (Key Mental Model)**
> The prod server materializes a derived DataFrame **only if some *active trigger* asks for
> it**, then each loop it reads the **last row** of that DF and emits an alert if the
> trigger's boolean column is `True`. Wiring a study in = (build it) → (register it) →
> (declare a trigger for it) → (activate the trigger). Nothing runs until that last step.

---

## The two runners

Both are real entry points in `pyproject.toml`:

| Command | File | Purpose |
|---|---|---|
| `poetry run analysis` | [run_analysis.py](run_analysis.py) | **Lab.** Exploratory scratchpad — build/inspect a study, print distributions, stop early. Emits no alerts. |
| `poetry run analysis_server` | [server.py](server.py) | **Prod.** The long-running loop that materializes registered DFs, evaluates active triggers, and emits alerts to outboxes. |

### Lab: `poetry run analysis`
[run_analysis.py](run_analysis.py) calls `get_clean_data()` (imported from `server.py`, so
you get the *identical* cleaned base_df the prod server uses), then **deliberately bypasses
the registry** — it imports a builder directly (e.g. `df = macd_entry(full_df)`), computes
columns inline, prints with `logdf(...)`/bucketizers, and `raise`s early to stop after you
eyeball the stats. This is where you *discover* which derived columns and boolean conditions
are worth turning into triggers. Edit it freely; it is a bench, not a contract.

### Prod: `poetry run analysis_server`
[server.py](server.py) runs the control-plane startup once, then loops. It does **not** import
builders directly — it only runs what the registries say to run (see below).

---

## The three "registries" (this is the confusing part)

There are three separate things called a "registry." They are distinct and live in different
places. Keeping them straight is the whole game:

| # | Name | Where | Holds | Who fills it |
|---|---|---|---|---|
| 1 | **`DF_REGISTRY`** | [df_materialization/registry.py](df_materialization/registry.py) | `df_name → builder fn` | Populated **at startup** by `df_registration_entrypoint()`. The file itself **must stay empty.** |
| 2 | **`TRIGGER_REGISTRY`** (+ `ACTIVE_TRIGGERS`) | `emit/outboxes/<outbox>/trigger_registry.py` and `.../active_triggers.py` | trigger specs; list of which are live | You, per outbox. |
| 3 | **outbox registry file** | `$QLIR_ALERTS_DIR/analysis_outboxes.json` on disk | which outboxes exist + their alert level | Written by `write_outbox_registry()` at startup; **read by the notification server** for discovery. |

### #1 — `DF_REGISTRY` must stay empty in the file
[registry.py](df_materialization/registry.py) defines `DF_REGISTRY = {}` and the comment says
*"THIS SHOULD REMAIN EMPTY."* It is populated **at runtime**: `server.py` calls
`df_registration_entrypoint()` ([registration.py](df_materialization/registration.py)), which
first runs `ensure_df_registry_empty_guard()` (raises `QLIRRegistrationError` if the dict was
pre-filled — i.e. if you wrongly put registrations in `registry.py`) and then calls
`register_df(name, builder=...)` for each DF. Register **in `registration.py`, never in
`registry.py`.**

### #2 — triggers are per-outbox, and `ACTIVE_TRIGGERS` is the on/off switch
Each outbox dir under [emit/outboxes/](emit/outboxes/) has two files:
- `trigger_registry.py` → `TRIGGER_REGISTRY` (a dict declaring every trigger that *could* fire)
- `active_triggers.py` → `ACTIVE_TRIGGERS` (a list of which trigger keys are *currently live*)

**These are intentionally empty (`{}` / `[]`) in a clean tree.** Empty active triggers ⇒ no
required DFs ⇒ nothing materializes ⇒ no alerts. That is the correct "nothing wired yet" state,
not a bug.

### #3 — the outbox registry file is for the notification server
At startup the server writes `analysis_outboxes.json` (via
[emit/alert.py](emit/alert.py) `write_outbox_registry`). The **notification server** reads it to
discover which outboxes exist. You don't edit this — it's generated from the outbox dirs.

---

## Adding a study from scratch (the promotion path)

Assume you know nothing about the system and want a new alert. Four steps:

### Step 0 — explore (lab)
In [run_analysis.py](run_analysis.py), load data with `get_clean_data()`, call your analysis
function, and inspect. Decide: (a) what the **derived DataFrame** is, and (b) the single
**boolean column** whose last-row value means "fire." Studies live under
[analyses/](analyses/) (e.g. [analyses/macd/macd_initial.py](analyses/macd/macd_initial.py)).

### Step 1 — add a builder
In [df_materialization/builders.py](df_materialization/builders.py), wrap your analysis entry
function. A builder takes the shared `base_df` and returns the derived DataFrame:

```python
def build_macd_1m(base_df: pd.DataFrame) -> pd.DataFrame:
    adf = df_macd_full_pyramidal_annotation(base_df)
    return macd_pyramid_perfect_frontside_plus_one_backside_light(adf)
```

Builders must be **deterministic and side-effect-free** (all start from the same `base_df`).

### Step 2 — register the builder
In [df_materialization/registration.py](df_materialization/registration.py), inside
`df_registration_entrypoint()`, add:

```python
register_df("1m_macd_with_pyramids", builder=build_macd_1m)
```

The string is the **`df_name`** — it's the key triggers will reference. (Do **not** add it to
`registry.py`.)

### Step 3 — declare a trigger and activate it
Pick the outbox by alert level (see [ALERT_LEVELS.md](ALERT_LEVELS.md)). In that outbox's
`trigger_registry.py`, add a spec; in its `active_triggers.py`, add the key:

```python
# emit/outboxes/qlir_events/trigger_registry.py
TRIGGER_REGISTRY = {
    "macd_pyramid_perfect": {
        "type": "df_column",                 # see "Trigger spec" below
        "description": "Perfect frontside + 1 light backside pyramid completed",
        "df": "1m_macd_with_pyramids",       # must match the registered df_name
        "column": "trigger_col_name",        # a single boolean column in that DF
    },
}

# emit/outboxes/qlir_events/active_triggers.py
ACTIVE_TRIGGERS = ["macd_pyramid_perfect"]
```

Restart `poetry run analysis_server`. That's it — the server now materializes
`1m_macd_with_pyramids` every loop and emits to `qlir-events` when the column's last row is
`True`.

---

## How the server runs it (what happens each loop)

Startup (once): `load_outboxes()` imports every outbox's `TRIGGER_REGISTRY`/`ACTIVE_TRIGGERS`
([emit/outboxes/load.py](emit/outboxes/load.py)) → `validate_*` ([emit/validate.py](emit/validate.py))
→ `df_registration_entrypoint()` fills `DF_REGISTRY` → `_collect_required_df_names()` walks
**active** triggers and collects each spec's `"df"` → `write_outbox_registry()`.

Per loop ([server.py](server.py)):
1. **Pipeline trust** — emit `data_stale` to `qlir-data-pipeline` if the latest row lags
   > `MAX_ALLOWED_LAG_SEC` (120s), with exponential backoff (30s → 15min; see
   [state/alerts.py](state/alerts.py)).
2. **Watermark gate** — skip if `data_ts <= last_processed_ts` (no new data).
3. **Materialize** the required DFs via [materialize.py](df_materialization/materialize.py)
   (missing `df_name` ⇒ hard `KeyError` = wiring bug).
4. **Events** — for active `qlir-events` triggers, read `df.iloc[-1][column]`; if `True`, add
   to `triggered_events` and emit.
5. **Non-event triggers** (tradable/positioning) — either read their own DF column, or compose
   from events (`events` + `events_condition`).
6. **Persist watermark.**

Key invariant: **only the last row matters** each loop, and a trigger DF must expose **exactly
one boolean trigger column**.

---

## Trigger spec reference (validated)

[emit/validate.py](emit/validate.py) enforces the shape. Every spec needs a non-empty
`description` and a `type` that is one of:

**`type: "df_column"`** — fires off a DataFrame column:
```python
{ "type": "df_column", "description": "...", "df": "<df_name>", "column": "<bool col>" }
# 'events' / 'events_condition' are forbidden for this type
```

**`type: "events"`** — composes from already-fired events (no DF of its own):
```python
{ "type": "events", "description": "...",
  "events": ["evt_a", "evt_b"], "events_condition": "ALL" }   # ALL | ANY | N_OF_M
# 'df' / 'column' are forbidden for this type
```

`ACTIVE_TRIGGERS` is also validated: unknown keys raise with a "did you mean…?" suggestion, and
duplicates raise.

> ⚠️ Gotcha: the old commented examples in some `trigger_registry.py` files use
> `"type": "signal"` / `"type": "survival rate"`. Those predate this validator and would now
> **fail** `validate_trigger_registry`. Use `df_column` or `events`.

---

## Operating it (start / stop / inspect)

**Start** — prod runs in its own tmux session (see [../start_all_simple.sh](../start_all_simple.sh),
which starts `qlir_analysis`, and [../tmux/analysis.sh](../tmux/analysis.sh)):
```bash
poetry run analysis_server                 # foreground
# or, as part of the pipeline:
../start_all_simple.sh                      # data → agg → analysis → notify (tmux)
tmux attach -t qlir_analysis                # watch it
```

**Stop**:
```bash
tmux kill-session -t qlir_analysis
```

**Inspect alerts** — the filesystem *is* the state (see [../README.md](../README.md)).
Alerts land under `$QLIR_ALERTS_DIR/<outbox>/`, and the notification server moves them to
`_sent/` or `_failed/`:
```bash
ls "$QLIR_ALERTS_DIR"/qlir-events/          # pending events alerts
ls "$QLIR_ALERTS_DIR"/_sent/qlir-events/    # delivered
cat "$QLIR_ALERTS_DIR"/analysis_outboxes.json   # what outboxes the server declared
```

**State files**: watermark + backoff persist across restarts (`STATE_PATH`
`~/.qlir/state/analysis_server.json`, and the alert backoff state). Deleting them just resets
the watermark/backoff — derived data is always rebuilt from the agg parquet window.

---

## Gotchas checklist

- `DF_REGISTRY` in [registry.py](df_materialization/registry.py) **stays empty**; register in
  [registration.py](df_materialization/registration.py). The guard will raise otherwise.
- Empty `TRIGGER_REGISTRY` / `ACTIVE_TRIGGERS` is the valid "nothing wired" state — the server
  will run but materialize nothing.
- A trigger's `"df"` must exactly match a registered `df_name`, or you get a `KeyError` at
  materialize time.
- The referenced `"column"` must be a single boolean column; only its **last row** is read.
- Use `type: "df_column"` or `type: "events"` — not the legacy `"signal"`.
- Config in [server.py](server.py) is currently hardcoded (`SOLUSDT 1m limit=1000`,
  `LAST_N_FILES=5`, `POLL_INTERVAL_SEC=15`); edit there to change the symbol/window.
