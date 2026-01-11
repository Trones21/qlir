# from datetime import timedelta
# import time
# from pathlib import Path
# import pandas as pd

# from qlir.data.core.paths import get_agg_dir_path
# from qlir.servers.analysis_server.state.alerts import INITIAL_BACKOFF, StaleAlertState, maybe_emit_stale_alert
# from qlir.utils.time.fmt import format_ts_human

# from .io.load_clean_data import load_clean_data
# from .state.progress import load_last_processed_ts, save_last_processed_ts
# from .checks.__data_staleness import is_data_stale, utc_now
# from .emit.alert import emit_alert


# import logging 
# log = logging.getLogger(__name__)

# PARQUET_CHUNKS_DIR = get_agg_dir_path("binance", "klines", "SOLUSDT", "1s", 1000)
# TRIGGER_COL = None
# MAX_ALLOWED_LAG = timedelta(minutes=5)
# TS_COL = "tz_start"


# LAST_N_FILES = 5
# POLL_INTERVAL_SEC = 30


# def main() -> None:


#     stale_state = load_stale_alert_state()
#     if stale_state is None:
#         stale_state = StaleAlertState(
#             last_emitted_at=None,
#             backoff_sec=INITIAL_BACKOFF,
#         )



#     while True:
#         df = load_clean_data(
#             PARQUET_CHUNKS_DIR,
#             last_n_files=LAST_N_FILES,
#         )

#         if df.empty:
#             log.info("Empty Dataframe")
#             time.sleep(POLL_INTERVAL_SEC)
#             continue

#         row = df.iloc[-1]
#         data_ts = pd.Timestamp(row[TS_COL], unit='ms', tz="UTC")

#         log.info(f"Rows: {len(df)} Most reccent row: {format_ts_human(data_ts)}")

#         # 1. Always evaluate staleness
#         is_stale = is_data_stale(data_ts=data_ts, max_lag=MAX_ALLOWED_LAG)

#         stale_state = maybe_emit_stale_alert(
#             is_stale=is_stale,
#             data_ts=data_ts,
#             now=utc_now(),
#             state=stale_state,
#         )

#         save_stale_alert_state(stale_state)



#         time.sleep(POLL_INTERVAL_SEC)


# if __name__ == "__main__":
#     main()