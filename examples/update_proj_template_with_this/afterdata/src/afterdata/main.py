# candles_from_disk_via_explicit_filepath
from pathlib import Path
from afterdata.logging.logging_setup import setup_logging, LogProfile

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_INFO)

import pandas as pd
import qlir.data.sources.load as load
from qlir.data.core.paths import get_agg_dir_path
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.core.datasource import DataSource
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
import qlir.data.quality.candles as DQ
from qlir.io.union_files import union_file_datasets
from qlir.utils.logdf import logdf

def entrypoint():
    print(f""""Hello World!
            Welcome to your QLIR analysis project, time to get to work!
            
            For Binance Data, first run the binance-data-server-arg to store the raw responses on disk.
            For Example:
            poetry run binance-data-server-arg --endpoint "klines" --symbol "SOLUSDT" --interval "1m" --limit "1000"
          
            Then run the binance-agg-server-arg to aggregate that data into parquets.
            For Example:
            poetry run binance-agg-server --endpoint "klines" --symbol "SOLUSDT" --interval "1m" --limit "1000"
          
            Note: You can only run one endpoint/symbol/interval/limit combo per data server or agg server
            Check pyproject.toml to see various ways to launch multiple data and agg servers via single poetry commands 

            Finally load your data into memory and do data quality checks, then once you are satisfied you can start your analysis!
          """)
        
    # Now you should have the data
    # the standard location is ~/qlir_data
    # ensure you have the data with: find . ~/qlir_data/

    # To load into a Dataframe call either:
    # candles_from_disk_via_built_filepath
    # Which takes: the datasource, symbol and resolution
    # this builds the canonical filepath for this dataset
    # and may use the corresponding dataset .meta.json for additional info
    
    # Or pass the filepath for a specific dataset explicitly
    # hint: use ls to find it
    # candles_from_disk_via_explicit_filepath

    # Optional - Step 2 Resample data (custom OHLCV candles)
    # df_resampled = build_custom_candles(df)

    # Enrich and analyze
    # for now, assume df is ready:
    # df = ...  # TODO: integrate your real pipeline, if you have many, you might consider creating a pipelines folder/module instead of placing it all here 
    # main(df)
    parquet_chunks_dir = get_agg_dir_path("binance", "klines", "SOLUSDT", "1m","1000")
    df = union_file_datasets(parquet_chunks_dir)
    print(df.columns)
    df.rename(columns={"open_time": "tz_start"}, inplace=True)
    logdf(df)
    fixed_df, dq_report = DQ.validate_candles(df, TimeFreq(1, TimeUnit.MINUTE))
    DQ.log_candle_dq_issues(dq_report)
    logdf(fixed_df, name="After DQ  ")

def main(df: pd.DataFrame):
    '''
    Core analysis body.

    - Add indicators / features / signals
    - Join with other datasets
    - Persist intermediate/final results
    - Optionally produce dataviz-friendly tables, plots, etc.
    '''
    # e.g.:
    # df = add_sma_slope_features(df)
    # df = compute_slope_persistence_stats(df)
    # df.to_parquet("data/processed/slope_persistence.parquet")

    return df  # useful for tests / notebooks


if __name__ == "__main__":
    entrypoint()

