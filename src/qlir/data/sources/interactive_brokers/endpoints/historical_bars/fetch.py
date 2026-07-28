from __future__ import annotations

import logging
from typing import Dict

from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.time.iso import now_iso

log = logging.getLogger(__name__)


class FetchFailed(Exception):
    def __init__(self, *, reason, meta=None, exc=None):
        self.reason = reason
        self.meta = meta
        self.exc = exc
        super().__init__(str(exc) if exc else str(reason))


def fetch(client, spec) -> tuple[list[list] | None, Dict | None, FetchFailed | None]:
    """
    Perform one historical-bars request via the (already connected) IBKR client.

    An empty result (rows == []) is NOT a failure — it takes the success path and the
    inspection layer decides whether that's a legitimately empty (closed-market) window
    or a still-forming one.
    """
    try:
        rows = client.req_historical_bars(
            end_dt=spec.end_dt,
            duration_str=spec.duration_str,
            bar_size_setting=spec.bar_size_setting,
        )
    except Exception as exc:
        reason = (
            SliceStatusReason.NETWORK_UNAVAILABLE
            if not client.is_connected()
            else SliceStatusReason.EXCEPTION
        )
        log.warning("IBKR historical fetch failed | reason=%s err=%s", reason, exc)
        return None, None, FetchFailed(
            reason=reason,
            meta={"completed_at": now_iso()},
            exc=exc,
        )

    return rows, {"completed_at": now_iso()}, None
