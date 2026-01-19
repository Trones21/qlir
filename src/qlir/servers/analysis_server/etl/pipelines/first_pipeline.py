import logging
from pathlib import Path

import pandas as pd

import qlir.data.quality.candles.candles as DQ
from qlir.logging.data_quality import log_data_staleness
from qlir.telemetry.telemetry import telemetry
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit

log = logging.getLogger(__name__)


@telemetry(console=True, log_path=Path("telemetry/etl_times.log"))
def clean_data(df):
    df.rename(columns={"open_time": "tz_start"}, inplace=True)
    clean_df, dq_report = DQ.validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))

    # Using tz_start as the index, but don't overwrite it b/c parquet write doesnt persist the index 
    # (even though the param is passed as true so idk...)
    clean_df["tz_idx"] = pd.to_datetime(clean_df["tz_start"], utc=True)
    clean_df = clean_df.set_index("tz_idx")

    DQ.log_candle_dq_issues(dq_report)
    log_data_staleness(df=clean_df)

    return clean_df