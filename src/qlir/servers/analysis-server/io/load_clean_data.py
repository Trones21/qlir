# analysis_server/io/load_clean_data.py

from pathlib import Path
import pandas as pd

from .parquet_window import load_latest_parquet_window
from .parquet_full import load_full_parquet  # optional later
from  ..etl.pipelines.first_pipeline import clean_data 


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
        df = load_latest_parquet_window(
            agg_dir,
            last_n_files=last_n_files,
        )
    else:
        df = load_full_parquet(agg_dir)

    if df.empty:
        return df

    df = clean_data(df)
    return df
