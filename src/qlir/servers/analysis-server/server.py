from datetime import timedelta
import time
from pathlib import Path
import pandas as pd

from qlir.data.core.paths import get_agg_dir_path

from .io.load_clean_data import load_clean_data
from .state.progress import load_last_processed_ts, save_last_processed_ts
from .checks.data_freshness import is_data_stale
from .emit.alert import emit_alert

PARQUET_CHUNKS_DIR = get_agg_dir_path("binance", "klines", "SOLUSDT", "1m", 1000)
TRIGGER_COL = "entry_signal"
MAX_ALLOWED_LAG = timedelta(minutes=5)
TS_COL = "tz_start"


LAST_N_FILES = 5
POLL_INTERVAL_SEC = 5.0


def main() -> None:
    last_processed_ts = load_last_processed_ts()

    while True:
        df = load_clean_data(
            PARQUET_CHUNKS_DIR,
            last_n_files=LAST_N_FILES,
        )

        if df.empty:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        row = df.iloc[-1]
        data_ts = pd.Timestamp(row[TS_COL], tz="UTC")

        if last_processed_ts and data_ts <= last_processed_ts:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # ---- data freshness check ----
        if is_data_stale(data_ts, max_lag=MAX_ALLOWED_LAG):
            emit_alert({
                "type": "data_stale",
                "data_ts": data_ts.isoformat(),
            })

        # ---- signal check ----
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