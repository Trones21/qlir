from __future__ import annotations

from datetime import datetime, timezone
import logging
import time
from typing import Any, Mapping

import pandas as pd
from qlir.io.writer import write
from qlir.servers.analysis_server.emit.alert import emit_alert, write_outbox_registry
from qlir.servers.analysis_server.emit.outboxes.load import load_outboxes
from qlir.servers.analysis_server.emit.validate import (
    validate_trigger_registry,
    validate_active_triggers,
)
from qlir.servers.analysis_server.df_materialization.registration import df_registration_entrypoint
from qlir.servers.analysis_server.df_materialization.materialize import materialize_required_dfs
from qlir.servers.analysis_server.io.load_clean_data import load_clean_data, wait_get_agg_dir_path
from qlir.servers.analysis_server.runtime_state import end_loop_and_sleep, update_runtime_state, runtime_state_get
from qlir.servers.analysis_server.state import (
    INITIAL_BACKOFF,
    AlertBackoffState,
    load_alert_states,
    maybe_emit_alert_with_backoff,
    save_alert_states,
)
from qlir.servers.analysis_server.state.progress import (
    load_last_processed_ts,
    save_last_processed_ts,
)

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Logging Level
# --------------------------------------------------------------------------

from qlir.servers.logging.logging_setup import LogProfile, setup_logging

# May want to abstract this away, curretly setting here for analysis server refactor
setup_logging(profile=LogProfile.QLIR_DEBUG)


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

PARQUET_CHUNKS_DIR = wait_get_agg_dir_path("binance", "klines", "SOLUSDT", "1m", 1000)
TS_COL = "tz_start"

POLL_INTERVAL_SEC = 15
LAST_N_FILES = 5
MAX_ALLOWED_LAG_SEC = 120

STATE_PATH = "~/.qlir/state/analysis_server.json"

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_data_stale(data_ts: datetime, max_lag_sec: int) -> bool:
    return (utc_now() - data_ts).total_seconds() > max_lag_sec


def get_clean_data() -> pd.DataFrame:
    return load_clean_data(
        PARQUET_CHUNKS_DIR,
        last_n_files=LAST_N_FILES,
    )


def _outbox_level(outbox_name: str) -> str:
    if outbox_name == "qlir-data-pipeline":
        return "pipeline"
    if outbox_name == "qlir-events":
        return "events"
    if outbox_name.startswith("qlir-tradable"):
        return "tradable"
    if outbox_name == "qlir-positioning":
        return "positioning"
    return "unknown"


def _collect_required_df_names(outboxes: Mapping[str, Mapping[str, Any]]) -> set[str]:
    """
    Determine which derived DFs are required by all active triggers.
    Composed (event-based) triggers contribute no DF requirement.
    """
    required: set[str] = set()

    for cfg in outboxes.values():
        registry = cfg["trigger_registry"]
        active = cfg["active_triggers"]

        for trigger_key in active:
            spec = registry[trigger_key]
            df_name = spec.get("df")
            events = spec.get("events")

            if events is not None:
                continue
            if df_name is not None:
                required.add(df_name)

    return required


def _last_row(df: pd.DataFrame, df_name: str) -> pd.Series:
    if df.empty:
        raise ValueError(f"Derived DF '{df_name}' is empty")
    return df.iloc[-1]


def _eval_events_condition(
    *,
    required_events: list[str],
    condition: str | None,
    triggered_events: set[str],
) -> bool:
    cond = (condition or "ALL").upper()
    req = set(required_events)

    if not req:
        return False

    if cond == "ALL":
        return req.issubset(triggered_events)
    if cond == "ANY":
        return not req.isdisjoint(triggered_events)

    raise ValueError(f"Unsupported events_condition: {condition!r}")


def handle_empty_base_df():
    now = utc_now()

    update_runtime_state("status.mode", "NO_DATA")
    update_runtime_state("status.reason", "base_df_empty")
    update_runtime_state("status.last_at", now.isoformat())

    # only set once
    if runtime_state_get("status.since") is None:
        update_runtime_state("status.since", now.isoformat())

    # Make it explicit that downstream is not current
    update_runtime_state("dfs.current", None)
    update_runtime_state("loop.skip_reason", "base_df_empty")

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> None:
    # ----------------------------------------------------------------------
    # Startup (control plane)
    # ----------------------------------------------------------------------

    outboxes = load_outboxes()

    for outbox_name, cfg in outboxes.items():
        validate_trigger_registry(cfg["trigger_registry"])
        validate_active_triggers(cfg["active_triggers"], cfg["trigger_registry"])

    # Register DF builders (CONTROL PLANE)
    df_registration_entrypoint()

    # Discover required DFs (static)
    required_df_names = _collect_required_df_names(outboxes)
    log.info("Required derived DFs: %s", sorted(required_df_names))

    # Write outbox registry for notification server discovery
    write_outbox_registry(
        {name: {"alert_level": _outbox_level(name)} for name in outboxes}
    )

    alert_states = load_alert_states()
    last_processed_ts = load_last_processed_ts()
    update_runtime_state("last_processed_ts", last_processed_ts)

    # ----------------------------------------------------------------------
    # Analysis loop
    # ----------------------------------------------------------------------

    while True:
        base_df = get_clean_data()
        if base_df.empty:
            handle_empty_base_df()
            end_loop_and_sleep(state_path=STATE_PATH, sleep_sec=POLL_INTERVAL_SEC)
            continue

        data_ts = base_df.iloc[-1][TS_COL]
        now = utc_now()

        # ------------------------------------------------------------------
        # Phase 1: pipeline triggers (trust)
        # ------------------------------------------------------------------

        stale_key = "data_stale"
        stale_state = alert_states.get(
            stale_key,
            AlertBackoffState(
                key=stale_key,
                last_emitted_at=None,
                backoff_sec=INITIAL_BACKOFF,
            ),
        )

        stale_state = maybe_emit_alert_with_backoff(
            state=stale_state,
            condition=is_data_stale(data_ts, MAX_ALLOWED_LAG_SEC),
            now=now,
            emit=lambda: emit_alert(
                outbox="qlir-data-pipeline",
                data={
                    "trigger": "data_stale",
                    "data_ts": data_ts.isoformat(),
                    "now": now.isoformat(),
                    "lag_sec": int((now - data_ts).total_seconds()),
                },
            ),
        )

        alert_states[stale_key] = stale_state
        save_alert_states(alert_states)

        # ------------------------------------------------------------------
        # Phase 2: watermark gate
        # ------------------------------------------------------------------

        if last_processed_ts is not None and data_ts <= last_processed_ts:
            end_loop_and_sleep(state_path=STATE_PATH, sleep_sec=POLL_INTERVAL_SEC)
            continue

        # ------------------------------------------------------------------
        # Phase 3: materialize derived DFs
        # ------------------------------------------------------------------

        derived_dfs = materialize_required_dfs(
            base_df=base_df,
            required_df_names=required_df_names,
        )
        update_runtime_state("materialized_dfs", derived_dfs)
        
        # ------------------------------------------------------------------
        # Phase 4: event evaluation
        # ------------------------------------------------------------------

        triggered_events: set[str] = set()

        events_cfg = outboxes.get("qlir-events")
        if events_cfg:
            registry = events_cfg["trigger_registry"]
            active = events_cfg["active_triggers"]
            update_runtime_state("triggers.all", registry)
            update_runtime_state("triggers.active", active)

            for trigger_key in active:
                spec = registry[trigger_key]
                df_name = spec["df"]
                col = spec["column"]

                last = _last_row(derived_dfs[df_name], df_name)

                if bool(last[col]):
                    triggered_events.add(trigger_key)
                    emit_alert(
                        outbox="qlir-events",
                        data={
                            "trigger": trigger_key,
                            "description": spec.get("description"),
                            "df": df_name,
                            "column": col,
                            "data_ts": data_ts.isoformat(),
                        },
                    )

        # ------------------------------------------------------------------
        # Phase 5: non-event triggers (tradable / positioning)
        # ------------------------------------------------------------------

        for outbox_name, cfg in outboxes.items():
            if outbox_name in ("qlir-events", "qlir-pipeline"):
                continue

            registry = cfg["trigger_registry"]
            active = cfg["active_triggers"]

            for trigger_key in active:
                spec = registry[trigger_key]

                df_name = spec.get("df")
                col = spec.get("column")
                events = spec.get("events")

                fired = False

                if events is not None:
                    fired = _eval_events_condition(
                        required_events=events,
                        condition=spec.get("events_condition"),
                        triggered_events=triggered_events,
                    )
                else:
                    last = _last_row(derived_dfs[df_name], df_name)
                    fired = bool(last[col])

                if fired:
                    emit_alert(
                        outbox=outbox_name,
                        data={
                            "trigger": trigger_key,
                            "description": spec.get("description"),
                            "df": df_name,
                            "column": col,
                            "events": events,
                            "data_ts": data_ts.isoformat(),
                        },
                    )

        # ------------------------------------------------------------------
        # Phase 6: persist watermark
        # ------------------------------------------------------------------

        save_last_processed_ts(data_ts)
        last_processed_ts = data_ts
        update_runtime_state("last_processed_ts", last_processed_ts)

        end_loop_and_sleep(state_path=STATE_PATH, sleep_sec=POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
