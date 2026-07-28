from __future__ import annotations

import logging
import time
from typing import Optional, Tuple

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def compute_time_range(client, now_ms: Optional[int] = None) -> Tuple[int, int]:
    """
    [start_ms, end_ms] to cover for this contract.

      - start_ms: earliest available data (IBKR reqHeadTimeStamp)
      - end_ms:   now

    The slicing layer (generate_bar_slices) partitions this into interval_ms*limit windows.
    """
    if now_ms is None:
        now_ms = _now_ms()

    earliest = client.req_head_timestamp_ms()
    if earliest is None:
        raise RuntimeError(
            "IBKR returned no head timestamp — no data, missing entitlement, or a bad "
            "contract. Check the symbol and your market-data subscriptions."
        )

    return earliest, now_ms
