# analysis_server/io/load_clean_data.py

import logging
from pathlib import Path
import time

import pandas as pd

from qlir.data.core.paths import get_agg_dir_path, get_data_root
from qlir.logging.ensure import ensure_logging
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
    
    wait_for_parquet_dir_ready(agg_dir)
    
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



def wait_for_parquet_dir_ready(
    path: Path,
    *,
    poll_seconds: float = 2.0,
    min_files: int = 1,
) -> None:
    ensure_logging()
    while True:
        if path.exists():
            files = list(path.glob("*.parquet"))
            if len(files) >= min_files:
                return

            log.info(
                "Parquet dir exists but empty (%d files) — waiting %.1fs",
                len(files),
                poll_seconds,
            )
        else:
            log.info(
                "Parquet directory not found yet: %s — waiting %.1fs",
                path,
                poll_seconds,
            )
            
        time.sleep(poll_seconds)


def wait_get_agg_dir_path(datasource:str, endpoint:str, symbol: str, interval: str, limit: int) -> Path:
    '''Just a wrapper so that the analysis server doesnt crash on startup if the path doesnt exist yet'''
    root = get_data_root()
    path = (Path(root)/datasource/endpoint/"agg"/symbol/interval/f"limit={limit}"/"parts")
    wait_for_parquet_dir_ready(path)
    path = get_agg_dir_path(datasource=datasource, endpoint=endpoint, symbol=symbol, interval=interval, limit=limit)
    return path