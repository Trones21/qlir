from __future__ import annotations

from datetime import datetime, timezone
import logging
import time

import pandas as pd

from qlir.data.core.paths import get_agg_dir_path
from qlir.servers.analysis_server.analyses.conduct_analysis import conduct_analysis
from qlir.servers.analysis_server.emit.alert import emit_alert
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

POLL_INTERVAL_SEC = 15
LAST_N_FILES = 5

ACTIVE_TRIGGERS=["sma_14_down_started", 
                 "sma_14_up_started",

                 "open_sma_14_up_10%_survive",
                 "open_sma_14_up_5%_survive",
                 "open_sma_14_up_1%_survive",
                 "open_sma_14_up_0.1%_survive",

                "open_sma_14_dn_10%_survive",
                 "open_sma_14_dn_5%_survive",
                 "open_sma_14_dn_1%_survive",
                 "open_sma_14_dn_0.1%_survive",
                 ]

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

    last_processed_ts = load_last_processed_ts()

    while True:

        df = get_clean_data()

        if df.empty:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        last_row = df.iloc[-1]
        data_ts = pd.Timestamp(last_row[TS_COL], unit="ms", tz="UTC")

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


        for col_str in ACTIVE_TRIGGERS:
            trig = TRIGGER_REGISTRY.get(col_str)

            if trig is None:
                raise KeyError(
                    f"Trigger column '{col_str}' in ACTIVE_TRIGGERS but not in registry"
                )
            log.info(f"last_row_aa :{last_row_aa.keys()}")
            if bool(last_row_aa[col_str]):
                emit_alert({
                    "trigger_col": col_str,
                    "type": trig["type"],
                    "description": trig["description"],
                    "data_ts": data_ts.isoformat(),
                    "last_row_open": last_row_aa["open"],
                    "row_before_that_open": second_last_row_aa["open"]
                })



        save_last_processed_ts(data_ts)
        last_processed_ts = data_ts

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
