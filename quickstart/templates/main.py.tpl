candles_from_disk_via_explicit_filepath
from sma_slope_persistence.logging_setup import setup_logging, LogProfile

# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_INFO)

import pandas as pd
import qlir.data.sources.load as load
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.core.datasource import DataSource
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit

def entrypoint():
    print(f""""Hello World!
            Welcome to your QLIR analysis project, time to get to work!

            Run fetch_initial_data to fetch data and store it on disk.
            Note: Current selection takes 20+ minutes to run, but since this will store the data on disk, you only have to run it once

            In the future you will run fetch_and_append_new_data  
          """)
        
    # Now you should have the data
    # the standard location is ~/qlir_data, and currently only DRIFT is supported/implemented
    # ensure you have the data with: ls ~/qlir_data/DRIFT/

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

