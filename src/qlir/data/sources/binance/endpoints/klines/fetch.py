
from typing import Dict

import httpx

from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.time.iso import now_iso


class FetchFailed(Exception):
    def __init__(self, *, reason, meta=None, exc=None):
        self.reason = reason
        self.meta = meta
        self.exc = exc
        super().__init__(str(exc) if exc else reason)


def fetch(url: str, timeout_sec: float) -> tuple[list[list] | None, Dict | None, FetchFailed | None]:
    # Perform the request
    completed_at = None
    http_status = None

    try:
        with httpx.Client(timeout=timeout_sec) as client:
            resp = client.get(url)
            completed_at = now_iso()
            http_status = resp.status_code
            resp.raise_for_status()

    except httpx.HTTPStatusError as exc:
        # HTTP response exists (4xx / 5xx)
        return None, None, FetchFailed(
            reason=SliceStatusReason.HTTP_ERROR,
            meta={
                "http_status": exc.response.status_code,
                "completed_at": now_iso(),
            },
            exc=exc,
        )

    except httpx.RequestError as exc:
        # No HTTP response exists (DNS, timeout, TCP, TLS, etc.)
        return None, None, FetchFailed(
            reason=SliceStatusReason.NETWORK_UNAVAILABLE,
            meta={
                "http_status": None,
                "completed_at": now_iso(),
            },
            exc=exc,
        )

    http_status = resp.status_code
    resp.raise_for_status()  # will raise on 4xx/5xx

    data = resp.json()

    return data, {"http_status": http_status, "completed_at": completed_at}, None


