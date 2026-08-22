---
name: qlir-new-analysis
description: Take a trading-analysis idea and wire it all the way into the QLIR analysis server so it emits live alerts - analysis function, DF builder, register_df, trigger spec, activation. Use when the user describes a setup, pattern, indicator condition, or signal they want detected, or asks to add a study, add a trigger, add an alert, or work out why a trigger never fires.
---

# Wiring a QLIR analysis from an idea to a live alert

Turn "I want to know when X happens" into a trigger the prod server evaluates
every loop.

Read `src/qlir/servers/analysis_server/RUNNING.md` alongside this — it is the
authoritative version and it is good. This skill is the working procedure.

## The mental model — state this before writing code

> The prod server materializes a derived DataFrame **only if some active trigger
> asks for it**. Each loop it reads the **last row** of that DF and emits an alert
> if the trigger's boolean column is `True`.

Two consequences that catch people:

- **Only the last row matters.** Whatever the study computes across history, the
  trigger reads `df.iloc[-1][column]`. The column must mean "fire *now*", not
  "fired somewhere in this window".
- **Nothing runs until activation.** Empty `TRIGGER_REGISTRY` / `ACTIVE_TRIGGERS`
  is the correct "nothing wired yet" state, not a bug.

## Step 0 — turn the idea into two concrete answers

Do not start writing until both are settled. Ask the user if the idea is vague.

1. **What derived DataFrame does this need?** Which indicator columns, over what
   base data.
2. **What single boolean column means "fire"?** One column. Named for the
   condition, e.g. `perfect_frontside_plus_1_light`.

If the idea needs cross-loop memory ("has been true for 3 bars"), that state
belongs **in the DataFrame** as a computed column. The server keeps no
cross-loop trigger state.

## Step 1 — explore in the lab

`poetry run analysis` runs `run_analysis.py`, which calls `get_clean_data()` —
the *same* cleaned base_df the prod server uses — then deliberately bypasses the
registry and imports a builder directly. Edit it freely; it is a bench, not a
contract. It ends in a `raise` to stop after you eyeball the stats.

Use `logdf(...)` and the bucketizers to check the condition actually fires at a
sane rate before wiring it. A trigger that fires every bar is noise; one that
fires twice a year is untestable.

Studies live under `analyses/`, e.g. `analyses/macd/macd_initial.py`. Follow that
file's shape: take `clean_data`, layer on indicator/feature helpers from the
library (`qlir.indicators`, `qlir.features`, `qlir.core`), return the frame.

**Build on the library, do not reimplement it.** Check `qlir/indicators/`,
`qlir/features/`, and `qlir/core/` before writing an indicator by hand.
`STRUCTURE.md` explains the layering; getting the layer right is most of the
battle.

## Step 2 — add a builder

In `df_materialization/builders.py`. A builder takes `base_df` and returns the
derived DataFrame:

```python
def build_macd_1m(base_df: pd.DataFrame) -> pd.DataFrame:
    adf = df_macd_full_pyramidal_annotation(base_df)
    return macd_pyramid_perfect_frontside_plus_one_backside_light(adf)
```

Requirements, enforced at materialize time:

- **Deterministic and side-effect free.** Every builder starts from the same
  `base_df`.
- **Must return a `pd.DataFrame`.** Several analysis functions return an
  `AnnotatedDF` — unwrap with `.df`. Returning the wrapper raises `TypeError`.

## Step 3 — register it

In `df_materialization/registration.py`, inside `df_registration_entrypoint()`:

```python
register_df("1m_macd_with_pyramids", builder=build_macd_1m)
```

**Register in `registration.py`, never in `registry.py`.** `registry.py` must stay
`DF_REGISTRY = {}`; `ensure_df_registry_empty_guard()` raises
`QLIRRegistrationError` if it was pre-filled. The string is the `df_name`
triggers reference.

## Step 4 — declare a trigger and activate it

Pick the outbox by **who should act** — see `ALERT_LEVELS.md`. Getting this wrong
is how people end up ignoring their own alerts:

| Outbox | Who acts | Use for |
|---|---|---|
| `qlir-events` | nobody directly | structural facts, treated like logs |
| `qlir-tradable-human` | a person | executable, persists long enough to act on |
| `qlir-tradable-binance-bot` | a bot | executable at machine speed |
| `qlir-positioning` | a person, urgently | position-level |
| `qlir-data-pipeline` | you, as operator | pipeline health |

Two files in `emit/outboxes/<outbox>/`:

```python
# trigger_registry.py  -- everything that COULD fire
TRIGGER_REGISTRY = {
    "perfect_macd_pyramid_frontside_reversal_point": {
        "type": "df_column",
        "description": "Perfect frontside either direction, plus the reversal signal",
        "df": "1m_macd_with_pyramids",     # must match the registered df_name
        "column": "perfect_frontside_plus_1_light",
    },
}

# active_triggers.py  -- what is LIVE right now
ACTIVE_TRIGGERS = ["perfect_macd_pyramid_frontside_reversal_point"]
```

Every spec needs a non-empty `description` and a `type` of either:

- `df_column` — `df` + `column`. `events` / `events_condition` forbidden.
- `events` — `events` (non-empty list) + `events_condition` (`ALL` | `ANY` |
  `N_OF_M`). Composes from already-fired events; `df` / `column` forbidden.

Legacy `"type": "signal"` / `"survival rate"` appear in old commented-out
examples and **fail validation now**. Do not copy them.

Restart `poetry run analysis_server`.

## Step 5 — verify it is actually wired

```bash
poetry run analysis_server            # watch startup
ls "$QLIR_ALERTS_DIR"/<outbox>/       # pending alerts
ls "$QLIR_ALERTS_DIR"/_sent/<outbox>/ # delivered
```

Startup logs the registered DFs and the required-DF set. If your `df_name` is not
in that set, the trigger is not active.

Alerts are only *delivered* if the outbox is routed in `notifications.toml` — the
notification server warns at startup about declared-but-unrouted outboxes. With
no config it routes everything to console, which is fine for testing. Use the
`qlir-notifications` skill for that side.

## Debugging: my trigger never fires

1. **`KeyError` on the df_name at materialize time** — the trigger's `"df"` does
   not match any `register_df` name. That is a wiring bug, and it is meant to be
   loud.
2. **Server runs but materializes nothing** — the trigger key is not in
   `ACTIVE_TRIGGERS`. Unknown keys raise with a "did you mean…?" suggestion, so a
   silent no-op means it is genuinely inactive.
3. **DF materializes, no alert** — the column's *last row* is not `True`. Check in
   the lab: `df.iloc[-1][column]`. A condition that was true mid-window but false
   on the final bar will never fire.
4. **Alerts in the outbox but nothing arrives** — that is delivery, not analysis.
   Switch to the `qlir-notifications` skill.
5. **Column is not boolean** — only one boolean column is read; a NaN or a
   non-bool will not behave.

## Gotchas

- `DF_REGISTRY` in `registry.py` stays empty; register in `registration.py`.
- A builder returning `AnnotatedDF` instead of `pd.DataFrame` raises `TypeError`.
- Server config is env-driven: `QLIR_ANALYSIS_SYMBOL`, `QLIR_ANALYSIS_INTERVAL`,
  `QLIR_ANALYSIS_LIMIT`, `QLIR_ANALYSIS_DATASOURCE`, `QLIR_ANALYSIS_ENDPOINT`
  (defaults: binance / klines / SOLUSDT / 1m / 1000). `RUNNING.md` still says
  these are hardcoded — it is out of date on that point.
- The analysis server blocks on startup until the agg server has produced
  parquet. That is deliberate. It logs what it is waiting on every 30s.
