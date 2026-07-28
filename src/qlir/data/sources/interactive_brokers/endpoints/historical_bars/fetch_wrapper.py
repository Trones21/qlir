from __future__ import annotations

import logging
from pathlib import Path
import time
from typing import Any, Dict, Optional

from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.time.iso import now_iso

from .fetch import FetchFailed, fetch
from .inspection import inspect_bars
from .persist import persist
from .request_spec import build_request_spec

log = logging.getLogger(__name__)


def fetch_and_persist_slice(
    *,
    client,
    request_slice_key: SliceKey,
    interval_ms: int,
    bar_size_setting: str,
    data_root: Optional[Path] = None,
    responses_dir: Optional[Path] = None,
    now_ms: Optional[int] = None,
) -> Dict[str, Any] | FetchFailed:
    """
    Fetch a single historical-bars slice via IBKR and write the raw response to disk.

    Called by the worker. Builds the request spec, performs the request via the (already
    connected) client, inspects completeness (equity-aware), and persists.
    """
    if responses_dir is None:
        raise ValueError("responses_dir must be provided to fetch_and_persist_slice")

    requested_at = now_iso()

    spec = build_request_spec(
        request_slice_key,
        bar_size_setting=bar_size_setting,
        what_to_show=client.what_to_show,
        use_rth=client.use_rth,
    )

    rows, success_info, fetch_fail = fetch(client, spec)

    if fetch_fail:
        return fetch_fail

    completed_at = success_info["completed_at"] if success_info else now_iso()
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    req_last_open_implicit = (request_slice_key.end_ms // interval_ms) * interval_ms

    inspection_result = inspect_bars(
        rows,
        requested_first_open=request_slice_key.start_ms,
        requested_last_open_implicit=req_last_open_implicit,
        interval_ms=interval_ms,
        now_ms=now_ms,
    )

    meta = persist(
        data=rows,
        request_slice_key=request_slice_key,
        responses_dir=responses_dir,
        data_root=data_root,
        inspection_result=inspection_result,
        contract_meta=client.contract_meta(),
        requested_at=requested_at,
        completed_at=completed_at,
    )

    return meta
