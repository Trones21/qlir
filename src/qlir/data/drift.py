from __future__ import annotations
from typing import Any, Dict, Optional
import time
import requests
import pandas as pd

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

def fetch_drift_candles(
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
        params["startTime"] = int(start_time)
    if end_time:
        params["endTime"] = int(end_time)

    print(f"[drift] Fetching {symbol} {res_tok}: start={start_time} end={end_time} limit={limit or 'default'}")

    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    records = payload.get("records", payload)
    df = pd.DataFrame(records)
    if df.empty:
        return df

    return normalize_candles(df, venue="drift", resolution=res_tok, include_partial=include_partial)


def fetch_drift_candles_all(
    symbol: str = "SOL-PERP",
    resolution: str | int = "1",
    *,
    start_time: Optional[int | float | pd.Timestamp] = None,
    end_time: Optional[int | float | pd.Timestamp] = None,
    chunk_limit: int = 1000,
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
    include_partial: bool = True,
    sleep_s: float = 0.15,
) -> pd.DataFrame:
    """
    Fetch candles across a full range.
    - If start_time is provided: page FORWARD (startTime → endTime).
    - If start_time is None:    page BACKWARD (… → endTime), moving endTime earlier each page.
    - Avoids infinite loops; merges/dedupes on tz_start.
    """
    from .normalize import normalize_candles

    sess = session or requests.Session()
    res_tok = normalize_drift_resolution_token(resolution)
    step_sec = _step_seconds_for_token(res_tok)
    st = _unix_s(start_time)
    et = _unix_s(end_time) or int(time.time())

    pages: list[pd.DataFrame] = []

    if st is not None:
        # ---------- Forward pagination ----------
        next_start = st
        prev_last_start_ts: Optional[int] = None
        stagnation_retries = 0
        MAX_STAGNATION_RETRIES = 2

        while True:
            params: Dict[str, Any] = {"limit": int(chunk_limit)}
            params["startTime"] = int(next_start)
            params["endTime"] = int(et)

            print(f"[drift] Fetching {symbol} {res_tok} (forward): start={params['startTime']} end={params['endTime']} limit={chunk_limit}")

            url = f"{DRIFT_BASE}/market/{symbol}/candles/{res_tok}"
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code == 400 and chunk_limit > 500:
                print("[drift] ⚠️ 400 Bad Request — reducing chunk_limit and retrying")
                chunk_limit = 500
                continue

            r.raise_for_status()
            payload = r.json()
            records = payload.get("records", payload)
            raw = pd.DataFrame(records)
            if raw.empty:
                print("[drift] (no more data)")
                break

            nd = normalize_candles(raw, venue="drift", resolution=res_tok, include_partial=include_partial)
            pages.append(nd)

            last_start = pd.to_datetime(nd["tz_start"].iloc[-1], utc=True)
            last_start_ts = int(last_start.timestamp())

            if prev_last_start_ts is not None and last_start_ts <= prev_last_start_ts:
                stagnation_retries += 1
                print(f"[drift] ⚠️ Non-advancing page (last_start={last_start_ts} ≤ prev={prev_last_start_ts}); forcing advance ({stagnation_retries}/{MAX_STAGNATION_RETRIES})")
                next_start = prev_last_start_ts + (step_sec or 1)
                if stagnation_retries > MAX_STAGNATION_RETRIES:
                    print("[drift] ⚠️ Stagnation persists — stopping to avoid infinite loop.")
                    break
            else:
                stagnation_retries = 0
                next_start = last_start_ts + (step_sec or 1)

            prev_last_start_ts = last_start_ts

            if next_start >= et:
                break
            time.sleep(sleep_s)

    else:
        # ---------- Backward pagination (when no start_time given) ----------
        # Start at 'et' (now by default), request newest page, then move endTime backward.
        cur_end = et
        prev_first_start_ts: Optional[int] = None
        stagnation_retries = 0
        MAX_STAGNATION_RETRIES = 2

        while True:
            params: Dict[str, Any] = {"limit": int(chunk_limit)}
            params["endTime"] = int(cur_end)

            print(f"[drift] Fetching {symbol} {res_tok} (backward): end={params['endTime']} limit={chunk_limit}")

            url = f"{DRIFT_BASE}/market/{symbol}/candles/{res_tok}"
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code == 400 and chunk_limit > 500:
                print("[drift] ⚠️ 400 Bad Request — reducing chunk_limit and retrying")
                chunk_limit = 500
                continue

            r.raise_for_status()
            payload = r.json()
            records = payload.get("records", payload)
            raw = pd.DataFrame(records)
            if raw.empty:
                print("[drift] (no more data)")
                break

            nd = normalize_candles(raw, venue="drift", resolution=res_tok, include_partial=include_partial)
            pages.append(nd)

            first_start = pd.to_datetime(nd["tz_start"].iloc[0], utc=True)
            first_start_ts = int(first_start.timestamp())

            # If we didn't move earlier, force a bigger jump
            if prev_first_start_ts is not None and first_start_ts >= prev_first_start_ts:
                stagnation_retries += 1
                print(f"[drift] ⚠️ Non-advancing page (first_start={first_start_ts} ≥ prev={prev_first_start_ts}); forcing step back ({stagnation_retries}/{MAX_STAGNATION_RETRIES})")
                cur_end = prev_first_start_ts - (step_sec or 1)
                if stagnation_retries > MAX_STAGNATION_RETRIES:
                    print("[drift] ⚠️ Stagnation persists — stopping to avoid infinite loop.")
                    break
            else:
                stagnation_retries = 0
                # Move endTime to just before the earliest candle we just received
                cur_end = first_start_ts - (step_sec or 1)

            prev_first_start_ts = first_start_ts

            # stop if we crossed the earliest possible unix or cur_end no longer decreases
            if cur_end <= 0:
                break
            time.sleep(sleep_s)

    if not pages:
        return pd.DataFrame()

    # For backward mode, pages are newest→oldest. Concatenate and sort ascending.
    out = pd.concat(pages, ignore_index=True)
    out = (
        out.sort_values("tz_start")
           .drop_duplicates(subset=["tz_start"], keep="last")
           .reset_index(drop=True)
    )
    return out


def fetch_drift_candles_update_from_df(
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
