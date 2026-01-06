# load, transform, emit (possibly multiple times)
# candles_from_disk_via_explicit_filepath
from pathlib import Path
from afterdata.logging.logging_setup import setup_logging, LogProfile

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.ALL_DEBUG)

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

@telemetry(console=True, log_path=Path("telemetry/etl_times.log"))
def clean_data():
    parquet_chunks_dir = get_agg_dir_path("binance", "klines", "SOLUSDT", "1m", 1000)
    df = union_file_datasets(parquet_chunks_dir)
    log.info(df.columns)
    df.rename(columns={"open_time": "tz_start"}, inplace=True)
    clean_df, dq_report = DQ.validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))

    # Using tz_start as the index, but don't overwrite it b/c parquet write doesnt persist the index 
    # (even though the param is passed as true so idk...)
    clean_df["tz_idx"] = pd.to_datetime(clean_df["tz_start"], utc=True)
    clean_df = clean_df.set_index("tz_idx")

    DQ.log_candle_dq_issues(dq_report)

    gap_fill_policy = ConstantFillPolicy()
    full_df_ = materialize_missing_rows(clean_df, interval_s=60)
    full_df_with_interpolated_gaps = apply_fill_policy(full_df_, interval_s=60, policy=gap_fill_policy)
    
    gap_verification_res = verify_gap_resolution(df_after_fill=full_df_with_interpolated_gaps, missing_count=len(dq_report.missing_starts), strict=True)
    log.info(gap_verification_res)
    if gap_verification_res.delta == 0:
        log.info(f"Gap fill verification passed. Applied Policy was ({type(gap_fill_policy)})")
    else:
        log.warning(f"Gap fill verification failed. Applied Policy was ({type(gap_fill_policy)})")
    
    log_data_staleness(df=full_df_with_interpolated_gaps)

    logdf(NamedDF(df=clean_df, name="After DQ"))
    logdf(NamedDF(df=full_df_with_interpolated_gaps, name="After Policy"))    
        
    write_tmp_parquet(full_df_with_interpolated_gaps)
    

def write_tmp_parquet(df):
    tmp_file = Path("~/sol1m.parquet")
    write(df, tmp_file)
    return 



def load_cleaned_data():
    tmp_file = Path("~/sol1m.parquet")
    sol_1m = read(tmp_file)
    return sol_1m