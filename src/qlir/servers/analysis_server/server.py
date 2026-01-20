from __future__ import annotations

from datetime import datetime, timezone
import logging
import time

import pandas as pd

from qlir.data.core.paths import get_agg_dir_path
from qlir.servers.analysis_server.analyses.conduct_analysis import conduct_analysis
from qlir.servers.analysis_server.emit.outboxes.load import load_outboxes
from qlir.servers.analysis_server.emit.validate import validate_trigger_registry, validate_active_triggers
from qlir.servers.analysis_server.emit.alert import emit_alert, write_outbox_registry
from qlir.servers.analysis_server.emit.trigger_registry import TRIGGER_REGISTRY
from qlir.servers.analysis_server.io.load_clean_data import load_clean_data, wait_get_agg_dir_path
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

PARQUET_CHUNKS_DIR = wait_get_agg_dir_path("binance", "klines", "SOLUSDT", "1m", 1000)
TS_COL = "tz_start"
MAX_ALLOWED_LAG = 120  # seconds

write_outbox_registry({
    "qlir-events": {"alert_level": "events"},
    "qlir-tradable-binance-bot": {"alert_level": "tradable"},
    "qlir-tradable-human": {"alert_level": "tradable"},
    "qlir-positioning": {"alert_level": "positioning"},
    "qlir-pipeline": {"alert_level": "pipeline"},
})

POLL_INTERVAL_SEC = 15
LAST_N_FILES = 5

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_data_stale(data_ts: datetime, max_lag: int) -> bool:
    return (utc_now() - data_ts).total_seconds() > max_lag


def get_clean_data(*args, **kwargs) -> pd.DataFrame:
    return load_clean_data(
            PARQUET_CHUNKS_DIR,
            last_n_files=LAST_N_FILES,
        )


def main() -> None:
    
    alert_states = load_alert_states()

    stale_state = alert_states.get(
        "data_stale",
        AlertBackoffState(
            key="data_stale",
            last_emitted_at=None,
            backoff_sec=INITIAL_BACKOFF,
        ),
    )
    
    outboxes = load_outboxes()
    for outbox_name, cfg in outboxes.items():
        validate_trigger_registry(cfg["trigger_registry"])
        validate_active_triggers(cfg["active_triggers"], cfg["trigger_registry"])

    last_processed_ts = load_last_processed_ts()

    while True:

        df = get_clean_data()

        if df.empty:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        last_row = df.iloc[-1]

       
        # Everything assumes tz_start is the open time column... (but im unsure if stuff expects unix ts int or Timestamp )
        # this might actually be something i need to formalize somewhere (conversion or whatever)
        # log.info(last_row)
        # log.info(f"================= {last_row[TS_COL]} {type(last_row[TS_COL])}")
        data_ts = last_row[TS_COL]

        now = utc_now()
        is_stale = is_data_stale(data_ts, MAX_ALLOWED_LAG)

        stale_state = maybe_emit_alert_with_backoff(
            state=stale_state,
            condition=is_stale,
            now=now,
            emit=lambda: emit_alert({
                "type": "data_stale",
                "data_ts": data_ts.isoformat(),
                "now": now.isoformat(),
                "lag_sec": int((now - data_ts).total_seconds()),
            }),
        )

        alert_states["data_stale"] = stale_state
        save_alert_states(alert_states)

       # 2. Only gate edge-triggered logic on watermark
        if last_processed_ts is not None and data_ts <= last_processed_ts:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        try:
            after_analysis = conduct_analysis(df)
            last_row_aa = after_analysis.iloc[-1]
            second_last_row_aa = after_analysis.iloc[-2]
        except Exception:
             log.error("Exception caught --- we may eventually remove this... but in the analysis path it is quite important, so im going to say that the process should crash")
             raise RuntimeError()
             time.sleep(POLL_INTERVAL_SEC)

        for outbox_name, cfg in outboxes.items():
            registry = cfg["trigger_registry"]
            active = cfg["active_triggers"]

            for trigger_key in active:
                trig = registry[trigger_key]

                if bool(last_row_aa[trigger_key]):
                    emit_alert(
                        outbox=outbox_name,
                        data={
                            "trigger": trigger_key,
                            "event_type": trig["event_type"],
                            "description": trig["description"],
                            "df": trig["df"],
                            "data_ts": data_ts.isoformat(),
                        },
                    )


        save_last_processed_ts(data_ts)
        last_processed_ts = data_ts

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
