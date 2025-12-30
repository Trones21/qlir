from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import parse_qs, urlparse
import logging

from qlir.data.sources.binance.endpoints.klines.inspection_result import inspect_res
from qlir.data.sources.binance.endpoints.klines.time_range import interval_to_ms
from qlir.data.sources.binance.intervals import floor_unix_ts_to_interval
from qlir.data.sources.common.slices.slice_key import SliceKey
log = logging.getLogger(__name__)
from qlir.data.sources.binance.endpoints.klines.deprec_rest_api_contracts import audit_binance_rest_kline_invariants
from qlir.time.iso import now_iso
from qlir.time.timeunit import TimeUnit
from qlir.utils.str.color import Ansi, colorize
from qlir.utils.str.fmt import term_fmt
from qlir.utils.time.fmt import format_ts_human
from qlir.utils.time.logging import TsDeltaResult, compute_ts_delta

from .urls import build_kline_url

try:  # pragma: no cover - external dependency wiring
    import httpx
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "httpx is required for qlir.data.sources.binance.endpoints.klines.fetch\n"
        "Install it with: pip install httpx"
    ) from exc


def make_canonical_slice_hash(slice_key: SliceKey) -> str:
    """
    Stable ID derived from the composite_key, used as filename.
    """
    key = slice_key.canonical_slice_composite_key().encode("utf-8")
    return hashlib.blake2b(key, digest_size=16).hexdigest()


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
) -> Dict[str, Any]:
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

    Returns:
        Dict with keys:
            - slice_id: str
            - relative_path: str
            - http_status: int
            - n_items: int
            - first_ts: int | None
            - last_ts: int | None
            - requested_at: ISO8601 str
            - completed_at: ISO8601 str
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

    # Perform the request
    with httpx.Client(timeout=timeout_sec) as client:
        resp = client.get(url)
    completed_at = now_iso()

    http_status = resp.status_code
    resp.raise_for_status()  # will raise on 4xx/5xx

    data = resp.json()
    
    # Inspect / Get an InspectionResult
    interval_ms = interval_to_ms(request_slice_key.interval)
    req_last_open_implicit = floor_unix_ts_to_interval(interval_in_ms=interval_ms,value_to_floor=request_slice_key.end_ms)
    inspection_result = inspect_res(raw=data, 
                                    requested_first_open=request_slice_key.start_ms, 
                                    requested_last_open_implicit=req_last_open_implicit,
                                    limit=request_slice_key.limit,
                                    interval_ms=interval_ms )

    # Prep for writing
    canonical_slice_compkey = request_slice_key.canonical_slice_composite_key()
    canonical_slice_compkey_hashed = make_canonical_slice_hash(request_slice_key)
    filename = f"{canonical_slice_compkey_hashed}.json"
    relative_path = f"responses/{filename}"
    file_path = responses_dir.joinpath(filename)

    # Wrap response with metadata so downstream consumers have context.
    raw_response_payload: Dict[str, Any] = {
        "meta": {
            "url": url,
            "slice_actual": request_slice_key.request_slice_composite_key(),
            "canoncal_slice": canonical_slice_compkey,
            "slice_id": canonical_slice_compkey_hashed,
            "symbol": request_slice_key.symbol,
            "interval": request_slice_key.interval,
            "request_param_startTime": format_ts_human(request_slice_key.start_ms),
            "request_param_endTime": format_ts_human(request_slice_key.end_ms),
            "requested_first_open": inspection_result.requested_first_open,
            "requested_last_open_implicit": inspection_result.requested_last_open_implicit,
            "limit": request_slice_key.limit,
            "http_status": http_status,
            "n_items": inspection_result.n_items,
            "slice_status": inspection_result.slice_status.value,
            "slice_status_reason": inspection_result.slice_status_reason.value,
            "received_first_open": inspection_result.received_first_open,
            "received_last_open": inspection_result.received_last_open,
            "requested_at": requested_at,
            "completed_at": completed_at,
            "data_root": str(data_root) if data_root is not None else None,
        },
        "data": data,
    }

    # Ensure directory exists and write to disk.
    responses_dir.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(raw_response_payload, f, indent=2)

    print(term_fmt(f"[{ colorize("WROTE", Ansi.BLUE)} - SLICE]: {file_path}"))

    # Return the metadata subset shape expected by worker.py
    return {
        "slice_id": canonical_slice_compkey_hashed,
        "relative_path": relative_path,
        "canonical_slice_comp_key": canonical_slice_compkey,

        "http_status": http_status,
        "url": url,

        # --- inspection truth ---
        "n_items": inspection_result.n_items,
        "received_first_open": inspection_result.received_first_open,
        "received_last_open": inspection_result.received_last_open,
        "requested_first_open": inspection_result.requested_first_open,
        "requested_last_open_implicit": inspection_result.requested_last_open_implicit,
        "slice_status": inspection_result.slice_status.value,
        "slice_status_reason": inspection_result.slice_status_reason.value,

        # --- raw request ---
        "request_param_startTime": format_ts_human(request_slice_key.start_ms),
        "request_param_endTime": format_ts_human(request_slice_key.end_ms),

        "requested_at": requested_at,
        "completed_at": completed_at,
    }

