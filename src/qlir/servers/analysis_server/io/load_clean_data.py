# analysis_server/io/load_clean_data.py

import logging
from pathlib import Path

import pandas as pd

from ..etl.pipelines.first_pipeline import clean_data

# from .parquet.window import load_latest_parquet_window
from .parquet import full, window

log = logging.getLogger(__name__)

def load_clean_data(
    agg_dir: Path,
    *,
    last_n_files: int,
) -> pd.DataFrame:
    """
    Load derived data from agg output, run ETL + verification,
    and return an analysis-ready DataFrame.
    """

    # policy decision (you can tune later)
    if last_n_files > 0:
        df = window.load_parquet_window(
            agg_dir_path=agg_dir,
            last_n_files=last_n_files,
        )
    else:
        log.info("Loading full parquet")
        df = full.load_parquet(agg_dir)

    if df.empty:
        return df

    df = clean_data(df)
    return df
