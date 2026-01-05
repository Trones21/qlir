from typing import Any, Dict, Optional

import pandas as _pd
import requests
import logging
log = logging.getLogger(__name__)
from .constants import DRIFT_BASE_URI, DRIFT_ALLOWED_RESOLUTIONS

def list_markets():
    raise NotImplementedError("Drift market discovery not implemented yet.")

def probe_candles_any_le(
    *,
    session: requests.Session,
    symbol: str,
    resolution: str,
    start_unix: int,
    timeout: float,
    include_partial: bool,
) -> Optional[int]:
    """
    Hit `/candles/{res_token}` with {'limit': 1, 'startTs': start_unix} 

    Returns:
        True if any record exists at start_unix, else None
    """
    log.debug("start_unix: %s", start_unix)
    params: Dict[str, Any] = {"limit": 1, "startTs": int(start_unix)}
    base_url = f"{DRIFT_BASE_URI}/market/{symbol}/candles/{resolution}"
    r = session.get(
        base_url,
        params=params,
        timeout=timeout,
    )
    if r.status_code == 400:
        log.debug("400 res")
        return None

    r.raise_for_status()
    payload = r.json()
    records = payload.get("records", payload)

    if not records:
        return None

    df = _pd.DataFrame(records)
    if df.empty:
        return None
    else:
        return True

def discover_earliest_candle_start(
    *,
    session: requests.Session,
    symbol: str,
    resolution: str,
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


    log.info(f"""
            [drift] Discovering earliest start (via startTs) for {symbol} {resolution} in [{lo}, {hi}]
            This may take a minute, pass LogProfile.ALL_DEBUG or LogProfile.QLIR_DEBUG for more granular logging""")

    while lo <= hi:
        mid = (lo + hi) // 2
        got_data = probe_candles_any_le(
            session=session,
            symbol=symbol,
            resolution=resolution,
            start_unix=mid,
            timeout=timeout,
            include_partial=include_partial,
        )
        log.debug(
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


