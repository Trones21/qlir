from pathlib import Path
import pandas as pd
import qlir.data.sources.load as load
from qlir.data.core.paths import get_agg_dir_path
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.core.datasource import DataSource
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
import qlir.data.quality.candles.candles as DQ
from qlir.data.quality.candles.verification import verify_gap_resolution
from qlir.io.writer import write
from qlir.io.reader import read
from qlir.io.union_files import union_file_datasets
from qlir.logging.logdf import logdf
from qlir.core.types.named_df import NamedDF
from qlir.data.lte.transform.gaps.materialization.materialize_missing_rows import materialize_missing_rows
from qlir.data.lte.transform.gaps.materialization.apply_fill_policy import apply_fill_policy
from qlir.data.lte.transform.policy.constant import ConstantFillPolicy
from qlir.logging.data_quality import log_data_staleness
from qlir.telemetry.telemetry import telemetry
import logging 
log = logging.getLogger(__name__)


# Super inefficient, but thats good... we want pressure to fix things lol

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

    return df