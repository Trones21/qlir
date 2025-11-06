from __future__ import annotations
from typing import Any, Dict, Optional
import time
import requests
import pandas as pd
import logging
log = logging.getLogger(__name__)
from qlir.utils.logdf import logdf

from drift_data_api_client import Client
from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
from qlir.data.candle_quality import validate_candles
from qlir.data.normalize import normalize_candles
from qlir.utils.logdf import logdf
from qlir.io.checkpoint import write_checkpoint, FileType
from qlir.io.union_files import union_file_datasets

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

def get_candles(symbol, resolution, from_ts: datetime, to_ts: datetime):
    
    client = Client("https://data.api.drift.trade")
    
    # Rename for clarity 
    intended_first_unix = from_ts
    intended_final_unix = to_ts 

    # wire format for the API (floor to seconds)
    intended_first_unix = int(intended_first_unix.timestamp())
    intended_final_unix = int(intended_final_unix.timestamp())

    temp_folder = f"tmp/{symbol}_{resolution}/"

    # need to first check if the intended_first_ts is within the range that the api provides 

    next_call_unix = intended_final_unix # seed for FIRST call only

    log.info("Paginiating backwards from: %s", datetime.fromtimestamp(intended_first_unix, timezone.utc) datetime.fromtimestamp(intended_first_ts, timezone.utc))
    
    pages: list[pd.DataFrame] = [] 
    earliest_got_ts = math.inf
    while earliest_got_ts > intended_first_unix:
        #log.info(f"earliest_got_ts: {earliest_got_ts} > anchor_ts: {math.floor(time.time())}")
        resp = get_market_symbol_candles_resolution.sync(symbol, client=client, resolution=GetMarketSymbolCandlesResolutionResolution(resolution), limit=20, start_ts=next_call_unix-1)
        if resp.records:
            resp_as_dict = resp.to_dict()
            page = pd.DataFrame(resp_as_dict["records"])
            clean = normalize_candles(page, venue="drift", resolution="60", keep_ts_start_unix=True, include_partial=False)
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
                write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname="")
                pages = []

    if pages:
        data = (
            pd.concat(pages, ignore_index=True)
            .sort_values("tz_start")
            .drop_duplicates(subset=["tz_start"], keep="last")
            .reset_index(drop=True)
        )
        write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname="tmp/SOL_1hr_partial_last")

    single_df = union_file_datasets("tmp") #note that this doesnt use read_candles but just read, could switch over later if we feel the need
    validate_candles(single_df)







# For Posterity
def old_fetch_drift_candles(
    symbol: str = "SOL-PERP",
    resolution: str | int = "1",
    limit: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
    *,
    include_partial: bool = True,
) -> pd.DataFrame:
    """
    Fetch a single page of candles from Drift.
    Returns a normalized DataFrame via normalize_candles().
    """
    from .normalize import normalize_candles  # lazy import

    sess = session or requests.Session()
    res_tok = normalize_drift_resolution_token(resolution)
    url = f"{DRIFT_BASE}/market/{symbol}/candles/{res_tok}"
    params: Dict[str, Any] = {}
    if limit:
        params["limit"] = int(limit)
    if start_time:
        params["startTs"] = int(start_time)
    if end_time:
        params["endTs"] = int(end_time)

    print(f"[drift] Fetching {symbol} {res_tok}: start={start_time} end={end_time} limit={limit or 'default'}")

    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    records = payload.get("records", payload)
    df = pd.DataFrame(records)
    if df.empty:
        return df

    return normalize_candles(df, venue="drift", resolution=res_tok, include_partial=include_partial)


def old_fetch_drift_candles_all(
    symbol: str = "SOL-PERP",
    resolution: str | int = "1",
    *,
    start_time: Optional[int | float | pd.Timestamp] = None,   # lower bound; if None we discover earliest
    end_time: Optional[int | float | pd.Timestamp] = None,     # optional upper bound
    chunk_limit: int = 1000,
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
    include_partial: bool = True,
    sleep_s: float = 0.15,
    catalog_min_unix: int = 1668470400,  # documented global minimum; per-symbol may be later
) -> pd.DataFrame:
    """
    Discover earliest data via binary search on endTs, then page FORWARD:

      Discovery (limit=1):
        - Binary search the smallest endTs that yields non-empty records.

      Pagination:
        - Use startTs=cursor and endTs=bound (now or provided) with limit=chunk_limit.
        - Advance cursor by the last page's max tz_start + step.

      Output:
        - Sorted ascending by tz_start, deduped on tz_start.
    """
    from .normalize import normalize_candles  # lazy import

    sess = session or requests.Session()
    res_tok = normalize_drift_resolution_token(resolution)
    step_sec = _step_seconds_for_token(res_tok) or 1

    now_unix = int(time.time())
    target_start = _unix_s(start_time) if start_time is not None else None
    target_end = _unix_s(end_time) if end_time is not None else now_unix

    # -------- Discover earliest available tz_start (unix) if needed --------
    if target_start is None:
        best = discover_earliest_candle_start(
            session=sess,
            symbol=symbol,
            res_token=res_tok,
            end_bound_unix=int(target_end),
            catalog_min_unix=int(catalog_min_unix),
            timeout=timeout,
            include_partial=include_partial
        )
        if best is None:
            # Nothing at/before end bound
            return pd.DataFrame()
        target_start = int(best)

    # -------- Forward pagination from target_start to target_end --------
    cursor = int(target_start)
    end_bound = int(target_end)

    pages: list[pd.DataFrame] = []
    prev_last_start_ts: Optional[int] = None
    stagnation_retries = 0
    MAX_STAGNATION_RETRIES = 2

    while True:
        params: Dict[str, Any] = {
            "limit": int(chunk_limit),
            "startTs": int(cursor),
            "endTs": int(end_bound),
        }

        log.info(f"[drift] Fetching {symbol} {res_tok} (forward): start={params['startTs']} end={params['endTs']} limit={chunk_limit}")

        r = sess.get(f"{DRIFT_BASE}/market/{symbol}/candles/{res_tok}", params=params, timeout=timeout)
        if r.status_code == 400 and chunk_limit > 500:
            log.info("[drift] ⚠️ 400 Bad Request — reducing chunk_limit and retrying")
            chunk_limit = 500
            continue
        r.raise_for_status()

        payload = r.json()
        records = payload.get("records", payload)
        raw = pd.DataFrame(records)
        if raw.empty:
            log.info("[drift] (no more data)")
            break

        nd = normalize_candles(raw, venue="drift", resolution=res_tok, include_partial=include_partial)
        if nd.empty:
            log.info("[drift] (normalized page empty)")
            break

        nd = nd.sort_values("tz_start").reset_index(drop=True)
        pages.append(nd)

        last_start_ts = int(nd["tz_start"].max().timestamp())

        # Stagnation guard
        if prev_last_start_ts is not None and last_start_ts <= prev_last_start_ts:
            stagnation_retries += 1
            log.info(
                f"[drift] ⚠️ Non-advancing page (last_start={last_start_ts} ≤ prev={prev_last_start_ts}); "
                f"forcing advance ({stagnation_retries}/{MAX_STAGNATION_RETRIES})"
            )
            cursor = prev_last_start_ts + step_sec
            if stagnation_retries > MAX_STAGNATION_RETRIES:
                log.info("[drift] ⚠️ Stagnation persists — stopping to avoid loop.")
                break
        else:
            stagnation_retries = 0
            cursor = last_start_ts + step_sec

        prev_last_start_ts = last_start_ts

        if cursor >= end_bound:
            break

        time.sleep(sleep_s)

    if not pages:
        return pd.DataFrame()

    out = (
        pd.concat(pages, ignore_index=True)
        .sort_values("tz_start")
        .drop_duplicates(subset=["tz_start"], keep="last")
        .reset_index(drop=True)
    )

    # Trim to bounds locally (in case first/last page overshot)
    if target_start is not None:
        out = out[out["tz_start"] >= pd.to_datetime(target_start, unit="s", utc=True)]
    if target_end is not None:
        out = out[out["tz_start"] <= pd.to_datetime(target_end, unit="s", utc=True)]

    return out.reset_index(drop=True)

def old_fetch_drift_candles_update_from_df(
    existing: Optional[pd.DataFrame],
    symbol: str = "SOL-PERP",
    resolution: Optional[str | int] = None,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
    include_partial: bool = True,
    chunk_limit: int = 1000,
    sleep_s: float = 0.15,
) -> pd.DataFrame:
    """
    Incrementally update an existing candles DataFrame.
    Uses last finalized candle (tz_end notna) as anchor.
    """
    sess = session or requests.Session()

    if existing is None or existing.empty:
        res_tok = normalize_drift_resolution_token(resolution or "1")
        return fetch_drift_candles(
            symbol=symbol,
            resolution=res_tok,
            session=sess,
            timeout=timeout,
            include_partial=include_partial,
        )

    if resolution is None:
        res_tok = infer_drift_resolution_token_from_df(existing)
    else:
        res_tok = normalize_drift_resolution_token(resolution)

    step_sec = _step_seconds_for_token(res_tok)
    ex = existing.sort_values("tz_start").copy()
    last_final_idx = ex["tz_end"].last_valid_index()
    if last_final_idx is not None:
        last_final_end = pd.to_datetime(ex.loc[last_final_idx, "tz_end"], utc=True)
        start_time = int(last_final_end.timestamp())
    else:
        last_start = pd.to_datetime(ex["tz_start"].iloc[-1], utc=True)
        start_time = int(last_start.timestamp()) + (step_sec or 1)

    print(f"[drift] Updating {symbol} {res_tok}: from {start_time} → now")

    new_df = fetch_drift_candles_all(
        symbol=symbol,
        resolution=res_tok,
        start_time=start_time,   # forward mode
        end_time=None,
        chunk_limit=chunk_limit,
        session=sess,
        timeout=timeout,
        include_partial=include_partial,
        sleep_s=sleep_s,
    )

    if new_df.empty:
        print("[drift] ✅ No new data.")
        return ex.reset_index(drop=True)

    merged = (
        pd.concat([ex, new_df], ignore_index=True)
        .sort_values("tz_start")
        .drop_duplicates(subset=["tz_start"], keep="last")
        .reset_index(drop=True)
    )

    print(f"[drift] ✅ Update complete — added {len(new_df)} rows, total {len(merged)}")
    return merged
