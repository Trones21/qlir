from __future__ import annotations

import time
import logging
from datetime import datetime, timezone

import pandas as pd

from qlir.data.core.paths import get_agg_dir_path
from qlir.servers.analysis_server.emit.alert import emit_alert
from qlir.servers.analysis_server.io.load_clean_data import load_clean_data
from qlir.servers.analysis_server.state import (
    AlertBackoffState,
    maybe_emit_alert_with_backoff,
    load_alert_states,
    save_alert_states,
    INITIAL_BACKOFF,
)
from qlir.servers.analysis_server.state.progress import load_last_processed_ts, save_last_processed_ts

log = logging.getLogger(__name__)

PARQUET_CHUNKS_DIR = get_agg_dir_path("binance", "klines", "SOLUSDT", "1s", 1000)
TS_COL = "tz_start"
MAX_ALLOWED_LAG = 60  # seconds

POLL_INTERVAL_SEC = 30
LAST_N_FILES = 5

TRIGGER_COL=None

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

        row = df.iloc[-1]
        data_ts = pd.Timestamp(row[TS_COL], unit="ms", tz="UTC")

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

        #     

       # 2. Only gate edge-triggered logic on watermark
        if last_processed_ts is not None and data_ts <= last_processed_ts:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # 3. Signals, etc.
        if TRIGGER_COL is not None:
            if bool(row[TRIGGER_COL]):
                emit_alert({
                    "type": "signal",
                    "data_ts": data_ts.isoformat(),
                    "trigger_col": TRIGGER_COL,
                })

        save_last_processed_ts(data_ts)
        last_processed_ts = data_ts

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
