from datetime import datetime
import json
from typing import Any, Dict
from qlir.data.sources.common.slices.canonical_hash import make_canonical_slice_hash
from qlir.utils.str.color import Ansi, colorize
from qlir.utils.str.fmt import term_fmt
from qlir.utils.time.fmt import format_ts_human


def persist(data, url, request_slice_key, responses_dir, data_root, inspection_result, http_status, requested_at, completed_at) -> Dict:
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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(term_fmt(f"{ts} [{ colorize("WROTE", Ansi.BLUE)} - SLICE]: {file_path}"))

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
