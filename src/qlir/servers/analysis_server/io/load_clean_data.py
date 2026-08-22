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
    log_every_seconds: float = 30.0,
) -> None:
    """
    Block until the agg server has written at least `min_files` parquet files.

    This wait is deliberate. The analysis server must not proceed without data,
    and it must not exit either -- that is what lets all four services be started
    at once, in any order, and restarted independently.

    Polling stays fast (`poll_seconds`) so the pipeline picks up as soon as data
    lands, but logging is throttled to `log_every_seconds` so a long wait does not
    bury the tmux pane. The first reason is always logged immediately, so it is
    never a mystery why the server is sitting still.
    """
    ensure_logging()

    waiting_since = time.monotonic()
    last_logged: float | None = None

    while True:
        if path.exists():
            files = list(path.glob("*.parquet"))
            if len(files) >= min_files:
                if last_logged is not None:
                    log.info(
                        "Parquet data is ready at %s (%d file(s)) after %.0fs — continuing",
                        path,
                        len(files),
                        time.monotonic() - waiting_since,
                    )
                return
            reason = f"parquet dir exists but holds {len(files)} file(s), need {min_files}"
        else:
            reason = "parquet directory does not exist yet"

        now = time.monotonic()
        if last_logged is None or (now - last_logged) >= log_every_seconds:
            log.info(
                "Waiting on upstream agg_server: %s (%s). Waited %.0fs; polling every %.1fs. "
                "This is expected until agg_server seals its first chunk.",
                path,
                reason,
                now - waiting_since,
                poll_seconds,
            )
            last_logged = now

        time.sleep(poll_seconds)


def wait_get_agg_dir_path(datasource:str, endpoint:str, symbol: str, interval: str, limit: int) -> Path:
    '''Just a wrapper so that the analysis server doesnt crash on startup if the path doesnt exist yet'''
    root = get_data_root()
    path = (Path(root)/datasource/endpoint/"agg"/symbol/interval/f"limit={limit}"/"parts")
    wait_for_parquet_dir_ready(path)
    path = get_agg_dir_path(datasource=datasource, endpoint=endpoint, symbol=symbol, interval=interval, limit=limit)
    return path