from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import parse_qs, urlparse
import logging

from qlir.data.sources.binance.endpoints.klines.inspection_result import inspect_res
from qlir.data.sources.binance.endpoints.klines.time_range import interval_to_ms
from qlir.data.sources.binance.intervals import floor_unix_ts_to_interval
from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.data.sources.binance.endpoints.klines.fetch import  fetch, FetchFailed
from qlir.data.sources.binance.endpoints.klines.persist import persist

log = logging.getLogger(__name__)
from qlir.time.iso import now_iso
from qlir.time.timeunit import TimeUnit
from qlir.utils.time.logging import compute_ts_delta

from .urls import build_kline_url

try:  # pragma: no cover - external dependency wiring
    import httpx
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "httpx is required for qlir.data.sources.binance.endpoints.klines.fetch\n"
        "Install it with: pip install httpx"
    ) from exc


def interval_to_timeunit(interval: str) -> TimeUnit | None:
    return {
        "1s": TimeUnit.SECOND,
        "1m": TimeUnit.MINUTE,
    }.get(interval)

def log_requested_slice_size(url: str):

    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    try:
        interval = params["interval"][0]
        unix_start = int(params["startTime"][0])
        unix_end = int(params["endTime"][0])
    except (KeyError, IndexError, ValueError) as exc:
        log.debug(f"Failed to parse slice parameters from url: {url}")
        return "<Error deriving requested slice size>"

    unit = interval_to_timeunit(interval)
    if unit is None:
        log.debug(
            f"Will not derive requested slice size; unsupported interval={interval}"
        )
        return "<Unsupported interval>"

    delta = compute_ts_delta(
        unix_a=unix_start,
        unix_b=unix_end,
        unit=unit,
    )

    log.debug(str(delta))
    return str(delta)


def fetch_and_persist_slice(
    request_slice_key: SliceKey,
    data_root: Optional[Path] = None,
    responses_dir: Optional[Path] = None,
    timeout_sec: float = 10.0,
) -> Dict[str, Any] | FetchFailed:
    """
    Fetch a single kline slice from Binance and write the raw response to disk.

    This function is designed to be called by the worker. It is responsible for:
      - building the URL
      - performing the HTTP request
      - extracting basic metadata (item count, first/last timestamps)
      - writing a JSON file under `responses_dir`
      - returning a metadata dict that the worker will embed into manifest.json

    Args:
        slice_key:
            KlineSliceKey describing the slice.
        data_root:
            Optional overall data root (not strictly needed here but included
            for future flexibility and debugging).
        responses_dir:
            Directory under which raw response files should be written.
            Typically: <data_root>/binance/klines/raw/<symbol>/<interval>/responses

        timeout_sec:
            HTTP request timeout in seconds.
    """
    if responses_dir is None:
        raise ValueError("responses_dir must be provided to fetch_and_persist_slice")

    requested_at = now_iso()
    url = build_kline_url(
        symbol=request_slice_key.symbol,
        interval=request_slice_key.interval,
        start_ms=request_slice_key.start_ms,
        end_ms=request_slice_key.end_ms,
        limit=request_slice_key.limit,
    )

    data, success_info, fetch_fail = fetch(url=url, timeout_sec=timeout_sec)
    
    if fetch_fail:
        return fetch_fail
        
    if success_info:
        # assert data is not None # To satisfy pylance...
        http_status = success_info['http_status']
        completed_at = success_info['completed_at']

        # Inspect / Get an InspectionResult
        interval_ms = interval_to_ms(request_slice_key.interval)
        req_last_open_implicit = floor_unix_ts_to_interval(interval_in_ms=interval_ms,value_to_floor=request_slice_key.end_ms)
        inspection_result = inspect_res(raw=data, 
                                        requested_first_open=request_slice_key.start_ms, 
                                        requested_last_open_implicit=req_last_open_implicit,
                                        limit=request_slice_key.limit,
                                        interval_ms=interval_ms)
        
        meta = persist(data=data, 
                url=url, 
                request_slice_key=request_slice_key, 
                data_root=data_root,
                responses_dir=responses_dir,
                inspection_result=inspection_result,
                http_status=http_status,
                requested_at=requested_at,
                completed_at=completed_at)

        return meta


    raise RuntimeError("Code must have been refactored... should have taken either the fail or success path (empty res takes the success path)")  

