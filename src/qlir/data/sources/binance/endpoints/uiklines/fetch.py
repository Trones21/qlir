from __future__ import annotations

import json
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from qlir.utils.str.color import Ansi, colorize
from qlir.utils.str.fmt import term_fmt
from qlir.utils.time.fmt import format_ts_human

from .model import UIKlineSliceKey
from .urls import build_UIKline_url

try:  # pragma: no cover - external dependency wiring
    import httpx
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "httpx is required for qlir.data.sources.binance.endpoints.UIKlines.fetch\n"
        "Install it with: pip install httpx"
    ) from exc


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_canonical_slice_hash(slice_key:UIKlineSliceKey) -> str:
    """
    Stable ID derived from the composite_key, used as filename.
    """
    key = slice_key.canonical_slice_composite_key().encode("utf-8")
    return hashlib.blake2b(key, digest_size=16).hexdigest()


def fetch_and_persist_slice(
    request_slice_key:UIKlineSliceKey,
    data_root: Optional[Path] = None,
    responses_dir: Optional[Path] = None,
    timeout_sec: float = 10.0,
) -> Dict[str, Any]:
    """
    Fetch a singleUIKline slice from Binance and write the raw response to disk.

    This function is designed to be called by the worker. It is responsible for:
      - building the URL
      - performing the HTTP request
      - extracting basic metadata (item count, first/last timestamps)
      - writing a JSON file under `responses_dir`
      - returning a metadata dict that the worker will embed into manifest.json

    Args:
        slice_key:
           UIKlineSliceKey describing the slice.
        data_root:
            Optional overall data root (not strictly needed here but included
            for future flexibility and debugging).
        responses_dir:
            Directory under which raw response files should be written.
            Typically: <data_root>/binance/Klines/raw/<symbol>/<interval>/responses

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

    requested_at = _now_iso()
    url = build_UIKline_url(
        symbol=request_slice_key.symbol,
        interval=request_slice_key.interval,
        start_ms=request_slice_key.start_ms,
        end_ms=request_slice_key.end_ms,
        limit=request_slice_key.limit,
    )

    # Perform the request
    with httpx.Client(timeout=timeout_sec) as client:
        resp = client.get(url)
    completed_at = _now_iso()

    http_status = resp.status_code
    resp.raise_for_status()  # will raise on 4xx/5xx

    data = resp.json()

    # BinanceUIKline format: list of lists.
    # Each entry: [ openTime, open, high, low, close, volume, closeTime, ... ]
    if isinstance(data, list) and data:
        n_items = len(data)
        first_ts = int(data[0][0])
        last_ts = int(data[-1][0])
    else:
        n_items = 0
        first_ts = None
        last_ts = None

    canonical_slice_compkey = request_slice_key.canonical_slice_composite_key()
    canonical_slice_compkey_hashed = make_canonical_slice_hash(request_slice_key)
    filename = f"{canonical_slice_compkey_hashed}.json"
    relative_path = f"responses/{filename}"
    file_path = responses_dir.joinpath(filename)

    # Wrap with metadata so downstream consumers have context.
    payload: Dict[str, Any] = {
        "meta": {
            "url": url,
            "slice_actual": request_slice_key.request_slice_composite_key(),
            "canoncal_slice": canonical_slice_compkey,
            "slice_id": canonical_slice_compkey_hashed,
            "symbol": request_slice_key.symbol,
            "interval": request_slice_key.interval,
            "start_ms": request_slice_key.start_ms,
            "end_ms": request_slice_key.end_ms,
            "limit": request_slice_key.limit,
            "http_status": http_status,
            "n_items": n_items,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "requested_at": requested_at,
            "completed_at": completed_at,
            "data_root": str(data_root) if data_root is not None else None,
        },
        "data": data,
    }

    # Ensure directory exists and write to disk.
    responses_dir.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(term_fmt(f"[{ colorize("WROTE", Ansi.BLUE)} - SLICE]: {file_path}"))
    print(term_fmt(f"    Canonical Slice Key:    {canonical_slice_compkey_hashed}"))
    print(term_fmt(f"    first candle: {format_ts_human(first_ts)}")) #type:ignore
    print(term_fmt(f"    last candle:  {format_ts_human(last_ts)}")) #type: ignore
    # Return the metadata subset expected by worker.py
    return {
        "slice_id": canonical_slice_compkey_hashed,
        "relative_path": relative_path,
        "http_status": http_status,
        "n_items": n_items,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "requested_at": requested_at,
        "completed_at": completed_at,
    }
