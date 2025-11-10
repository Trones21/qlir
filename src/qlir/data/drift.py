from __future__ import annotations
from typing import Any, Dict, Optional
import time
import requests
import pandas as pd
import logging
log = logging.getLogger(__name__)
from qlir.utils.logdf import logdf
from pathlib import Path
from drift_data_api_client import Client
from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
from qlir.data.candle_quality import validate_candles, infer_freq
from qlir.data.normalize import normalize_candles
from qlir.utils.logdf import logdf
from qlir.io.checkpoint import write_checkpoint, FileType
from qlir.io.union_files import union_file_datasets
from qlir.io.writer import write, write_dataset_meta, _prep_path
from qlir.io.reader import read
from qlir.data.load import get_symbol
from qlir.df.utils import union_and_sort
from datetime import datetime, timezone
import math


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DRIFT_BASE = "https://data.api.drift.trade"
DRIFT_ALLOWED = {"1", "5", "15", "60", "240", "D", "W", "M"}


# ---------------------------------------------------------
# Resolution Helpers
# ---------------------------------------------------------

def normalize_drift_resolution_token(res: str | int) -> str:
    """
    Accept flexible inputs (1, "1m", "60", "D", "W", "M", 3600s, etc.)
    and return one of: {"1","5","15","60","240","D","W","M"}.
    """
    if isinstance(res, int):
        if res in (1, 5, 15, 60, 240):
            return str(res)
        if res % 60 == 0:
            mins = res // 60
            if mins in (1, 5, 15, 60, 240):
                return str(mins)
        raise ValueError(f"Unrecognized numeric resolution: {res}")

    r = str(res).strip().lower()

    if r in {"d", "day", "daily"}:
        return "D"
    if r in {"w", "wk", "week", "weekly"}:
        return "W"
    if r in {"m", "mo", "mon", "month", "monthly"}:
        return "M"

    if r.endswith("m"):
        mins = int(r[:-1])
        if mins in (1, 5, 15, 60, 240):
            return str(mins)
    if r.endswith("h"):
        hrs = int(r[:-1])
        mins = hrs * 60
        if mins in (60, 240):
            return str(mins)

    if r.isdigit():
        if r in DRIFT_ALLOWED:
            return r
        mins = int(r)
        if mins in (1, 5, 15, 60, 240):
            return str(mins)

    raise ValueError(
        f"Resolution {res!r} is not supported. Use one of {sorted(DRIFT_ALLOWED)} or compatible aliases."
    )


def infer_drift_resolution_token_from_df(df: pd.DataFrame) -> str:
    """
    Infer Drift resolution token from tz_start diffs.
    Returns one of {"1","5","15","60","240","D","W","M"}.
    """
    if df.empty:
        raise ValueError("Cannot infer resolution from empty DataFrame.")

    ts = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(ts) < 2:
        raise ValueError("Need at least two rows to infer resolution.")

    diffs = ts.diff().dropna().dt.total_seconds().astype(int)
    step = int(diffs.mode().iloc[0]) if not diffs.mode().empty else int(diffs.min())

    sec_to_token = {
        60: "1",
        300: "5",
        900: "15",
        3600: "60",
        14400: "240",
        86400: "D",
        604800: "W",
    }
    if step in sec_to_token:
        return sec_to_token[step]

    day = 86400
    if 25 * day <= step <= 35 * day:
        return "M"

    raise ValueError(f"Could not map inferred step {step}s to Drift token.")


def _step_seconds_for_token(token: str) -> Optional[int]:
    """Nominal step size for each token (None for D/W/M)."""
    minute_map = {"1": 60, "5": 300, "15": 900, "60": 3600, "240": 14400}
    return minute_map.get(token)


# ---------------------------------------------------------
# Time helpers
# ---------------------------------------------------------

def _unix_s(x: pd.Timestamp | int | float | None) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, pd.Timestamp):
        return int(x.tz_convert("UTC").timestamp())
    x = float(x)
    return int(x / 1000.0) if x > 1_000_000_000_000 else int(x)


# ---------------------------------------------------------
# Core fetchers
# ---------------------------------------------------------




def probe_candles_any_le(
    *,
    session: requests.Session,
    symbol: str,
    res_token: str,
    start_unix: int,
    timeout: float,
    include_partial: bool,
) -> Optional[int]:
    """
    Hit `/candles/{res_token}` with {'limit': 1, 'startTs': start_unix} 

    Returns:
        True if any record exists at start_unix, else None
    """
    from .normalize import normalize_candles  # lazy import to avoid import cycles
    log.info("start_unix: %s", start_unix)
    params: Dict[str, Any] = {"limit": 1, "startTs": int(start_unix)}
    base_url = f"{DRIFT_BASE}/market/{symbol}/candles/{res_token}"
    r = session.get(
        base_url,
        params=params,
        timeout=timeout,
    )
    if r.status_code == 400:
        log.info("400 res")
        return None

    r.raise_for_status()
    payload = r.json()
    records = payload.get("records", payload)

    if not records:
        return None

    df = pd.DataFrame(records)
    if df.empty:
        return None
    else:
        return True

def discover_earliest_candle_start(
    *,
    session: requests.Session,
    symbol: str,
    res_token: str,
    end_bound_unix: int,
    catalog_min_unix: int,
    timeout: float,
    include_partial: bool,
) -> Optional[int]:
    """
    Binary-search the smallest `startTs` that yields a page, then return the
    earliest tz_start (unix) observed.

    Args:
        session: requests.Session to reuse connections
        symbol: e.g. 'SOL-PERP'
        res_token: normalized resolution token (e.g., '1', '5', '1H', etc.)
        end_bound_unix: upper search bound (inclusive); usually now or a provided end_time
        catalog_min_unix: documented global minimum (api not tested with values beneath this)
        timeout: request timeout
        include_partial: forward to normalizer

    Returns:
        earliest tz_start (unix) if found, else None
    """
    lo = int(catalog_min_unix)
    hi = int(end_bound_unix)
    best: Optional[int] = None


    log.info(f"[drift] Discovering earliest start (via startTs) for {symbol} {res_token} in [{lo}, {hi}]")

    while lo <= hi:
        mid = (lo + hi) // 2
        got_data = probe_candles_any_le(
            session=session,
            symbol=symbol,
            res_token=res_token,
            start_unix=mid,
            timeout=timeout,
            include_partial=include_partial,
        )
        log.info(
            f"[drift] probe startTs={mid} -> {'hit' if got_data is not None else 'empty'}"
        )
        
        # Found data, take lower half
        if got_data is not None:
            hi = mid - 1
            best = mid

        # No data, take upper half
        if got_data is None:
            lo = mid + 1


    if best is None:
        log.info("[drift] No data found up to end bound.")
    else:
        log.info(f"[drift] Earliest available ts(unix) = {best}")

    return best

# -----------------------------------------------------------------------------
# Main fetch with forward pagination
# -----------------------------------------------------------------------------

def get_candles_since():
    return

def get_candles_all():
    return

def add_new_candles_to_dataset(existing_file: str, symbol_override: str | None = None):
    dataset_uri = Path(existing_file)
    existing_df = read(dataset_uri)
    
    log.info("Currently using infer_freq only, not a full data quality check - may later change to full candle_quality func")
    resolution = infer_freq(existing_df)
    log.info 
    current_last_candle = existing_df["tz_start"].max()
    
    #Try to get symbol 
    symbol: str | None = get_symbol(dataset_uri)    
    if symbol is None and symbol_override is None:
        raise Exception("Symbol cannot be inferred from existing_file or associated .meta.json and no symbol_override was passed, we therefore do not know which symbol to retrieve data for")
    if symbol is None:
        effective_symbol = symbol_override
    else:
        effective_symbol = symbol

    log.info(f"Getting new {resolution} {effective_symbol} candles since {current_last_candle}")
    resolution_param = GetMarketSymbolCandlesResolutionResolution(resolution)
    new_candles_df = get_candles(effective_symbol, resolution=resolution_param, from_ts=current_last_candle )
    full_df = union_and_sort([existing_df, new_candles_df], sort_by=["tz_start"])
    write(full_df, dataset_uri)
    write_dataset_meta(dataset_uri, symbol=effective_symbol, resolution=resolution)
    return full_df

def get_candles(symbol, resolution: GetMarketSymbolCandlesResolutionResolution, from_ts: datetime, to_ts: datetime | None = None, save_dir: str = ".", filetype_out: FileType = FileType.PARQUET):
    
    client = Client("https://data.api.drift.trade")
    
    # Handle to_ts first (using current time if None)
    intended_final_unix = datetime.now(timezone.utc) if to_ts is None else to_ts
    intended_final_unix = int(intended_final_unix.timestamp())

    # need to first check if the intended_first_ts is within the range that the api provides 


    # Handle from_ts: if None, discover earliest candle start
    if from_ts is None:
        raise Exception("Discover earliest candle start not integrated with get candles, need to ensure the resolution map works correctly")
        # discovered = discover_earliest_candle_start(
        #     session=session,
        #     symbol=symbol,
        #     res_token=res_token,
        #     end_bound_unix=intended_final_unix,
        #     catalog_min_unix=catalog_min_unix,
        #     timeout=timeout,
        #     include_partial=include_partial,
        # )
        # intended_first_unix = discovered
    else:
        intended_first_unix = int(from_ts.timestamp())

    
    temp_folder = f"tmp/{symbol}_{resolution}/"

    

    next_call_unix = intended_final_unix # seed for FIRST call only

    log.info("Paginiating backwards from: %s", datetime.fromtimestamp(intended_final_unix, timezone.utc))
    
    pages: list[pd.DataFrame] = [] 
    earliest_got_ts = math.inf
    while earliest_got_ts > intended_first_unix:
        #log.info(f"earliest_got_ts: {earliest_got_ts} > anchor_ts: {math.floor(time.time())}")
        resp = get_market_symbol_candles_resolution.sync(symbol, client=client, resolution=GetMarketSymbolCandlesResolutionResolution(resolution), limit=20, start_ts=next_call_unix-1)
        if resp.records:
            resp_as_dict = resp.to_dict()
            page = pd.DataFrame(resp_as_dict["records"])
            clean = normalize_candles(page, venue="drift", resolution=resolution, keep_ts_start_unix=True, include_partial=False)
            sorted = clean.sort_values("tz_start").reset_index(drop=True)
            pages.append(sorted)
            
            # currently going to paginate backward from final_ts
            first_row = sorted.iloc[0]
            last_row =  sorted.iloc[-1]
            log.info(f"Retrieved bars starting with: {first_row['tz_start']} to {last_row['tz_start']}")
            # logdf(sorted, 25)
            earliest_got_ts = first_row['ts_start_unix']
            next_call_unix = earliest_got_ts - 1 # remove 1 second to avoid duplicating a row
            if len(pages) > 2:
                data = (
                    pd.concat(pages, ignore_index=True)
                    .sort_values("tz_start")
                    .drop_duplicates(subset=["tz_start"], keep="last")
                    .reset_index(drop=True)
                )
                write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname=temp_folder)
                pages = []

    if pages:
        data = (
            pd.concat(pages, ignore_index=True)
            .sort_values("tz_start")
            .drop_duplicates(subset=["tz_start"], keep="last")
            .reset_index(drop=True)
        )
        write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname=temp_folder)

    single_df = union_file_datasets("tmp") #note that this doesnt use read_candles but just read, could switch over later if we feel the need
    validate_candles(single_df)

    # We should verify the range 

    #Write the single file
    clean_dirpath = _prep_path(save_dir)
    dataset_uri = f"{clean_dirpath}/{symbol}_{resolution}.{filetype_out}"
    write(single_df, dataset_uri)
    write_dataset_meta(dataset_uri, symbol=symbol, resolution=resolution)
    return single_df

