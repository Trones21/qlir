from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Dict

from qlir.data.sources.common.slices.canonical_hash import make_canonical_slice_hash


def persist(
    *,
    data,
    request_slice_key,
    responses_dir,
    data_root,
    inspection_result,
    contract_meta: Dict[str, Any],
    requested_at: str,
    completed_at: str,
) -> Dict:
    """
    Write the raw IBKR bar slice to responses/<slice_id>.json as {meta, data} and return
    the metadata subset the worker embeds into the manifest. Mirrors the Binance persist,
    minus HTTP/URL fields (IBKR has neither) and plus the IBKR contract identity.
    """
    canonical_slice_compkey = request_slice_key.canonical_slice_composite_key()
    slice_id = make_canonical_slice_hash(request_slice_key)
    filename = f"{slice_id}.json"
    relative_path = f"responses/{filename}"
    file_path = responses_dir.joinpath(filename)

    raw_response_payload: Dict[str, Any] = {
        "meta": {
            "source": "interactive_brokers",
            "endpoint": "historical_bars",
            "slice_actual": request_slice_key.request_slice_composite_key(),
            "slice_comp_key": canonical_slice_compkey,
            "slice_id": slice_id,
            "symbol": request_slice_key.symbol,
            "interval": request_slice_key.interval,
            "limit": request_slice_key.limit,
            "contract": contract_meta,
            "request_param_start_ms": request_slice_key.start_ms,
            "request_param_end_ms": request_slice_key.end_ms,
            "requested_first_open": inspection_result.requested_first_open,
            "requested_last_open_implicit": inspection_result.requested_last_open_implicit,
            "n_items": inspection_result.n_items,
            "slice_status": inspection_result.slice_status.value,
            "slice_status_reason": inspection_result.slice_status_reason.value,
            "received_first_open": inspection_result.received_first_open,
            "received_last_open": inspection_result.received_last_open,
            "bar_columns": [
                "open_time", "open", "high", "low", "close", "volume", "average", "bar_count",
            ],
            "requested_at": requested_at,
            "completed_at": completed_at,
            "data_root": str(data_root) if data_root is not None else None,
        },
        "data": data,
    }

    responses_dir.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(raw_response_payload, f, indent=2)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{ts} [WROTE - SLICE]: {file_path}")

    return {
        "slice_id": slice_id,
        "relative_path": relative_path,
        "slice_comp_key": canonical_slice_compkey,
        "n_items": inspection_result.n_items,
        "received_first_open": inspection_result.received_first_open,
        "received_last_open": inspection_result.received_last_open,
        "requested_first_open": inspection_result.requested_first_open,
        "requested_last_open_implicit": inspection_result.requested_last_open_implicit,
        "slice_status": inspection_result.slice_status.value,
        "slice_status_reason": inspection_result.slice_status_reason.value,
        "first_ts": request_slice_key.start_ms,
        "last_ts": request_slice_key.end_ms,
        "http_status": None,
        "url": None,
        "requested_at": requested_at,
        "completed_at": completed_at,
    }
