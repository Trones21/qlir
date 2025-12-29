from __future__ import annotations

import math
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import httpx

from qlir.data.sources.binance.intervals import interval_to_ms

from .urls import build_kline_url
import logging
log = logging.getLogger(__name__)

def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def probe_earliest_open_time(
    symbol: str,
    interval: str,
    timeout_sec: float = 10.0,
) -> Optional[int]:
    """
    Probe Binance for the earliest available kline openTime for a symbol+interval.

    Strategy:
      - Request 1 candle starting from Unix epoch (startTime=0, limit=1).
      - If Binance has data, it should return the earliest kline >= 0.
      - If the response is empty or an error occurs, return None.

    Returns:
        openTime in milliseconds, or None if no data found.
    """
    # startTime=0, no endTime, limit=1
    url = build_kline_url(
        symbol=symbol,
        interval=interval,
        start_ms=0,
        end_ms=None,
        limit=1,
    )

    with httpx.Client(timeout=timeout_sec) as client:
        resp = client.get(url)

    if resp.status_code == 404:
        # Symbol might be unknown / delisted with no history at all.
        return None

    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, list) or not data:
        return None

    # Kline format: [ openTime, open, high, low, close, volume, closeTime, ... ]
    return int(data[0][0])


def probe_latest_open_time(
    symbol: str,
    interval: str,
    timeout_sec: float = 10.0,
    now_ms: Optional[int] = None,
) -> Optional[int]:
    """
    Probe Binance for the latest available kline openTime for a symbol+interval.

    Strategy:
      - Request 1 candle with endTime ≈ now (limit=1, endTime=now_ms).
      - Binance should return the last candle whose openTime < endTime.
      - If the response is empty or an error occurs, return None.

    Returns:
        openTime in milliseconds, or None if no data found.
    """
    if now_ms is None:
        now_ms = _now_ms()
    
    # extra guard in case type is ignored and a float is passed
    now_ms = math.floor(now_ms)

    url = build_kline_url(
        symbol=symbol,
        interval=interval,
        start_ms=None,
        end_ms=now_ms,
        limit=1,
    )

    with httpx.Client(timeout=timeout_sec) as client:
        resp = client.get(url)

    if resp.status_code == 404:
        return None

    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, list) or not data:
        return None

    return int(data[-1][0])


def compute_time_range(
    symbol: str,
    interval: str,
    limit: int,
    now_ms: Optional[int] = None,
    timeout_sec: float = 10.0,
) -> Tuple[int, int]:
    """
    Compute the [start_ms, end_ms] range to cover for this symbol+interval.

    Semantics (for now):

      - start_ms:
            Earliest available kline openTime from Binance
            (based on a probe at startTime=0, limit=1).

      - end_ms:
            Usually "current" latest kline openTime (based on a probe with
            limit=1 & endTime≈now_ms). For delisted symbols with no recent
            data, this will be the last historical openTime.

      - Slice alignment:
            For now we simply return [start_ms, end_ms]. The slicing layer
            (generate_kline_slices) will partition this into spans of
            interval_ms * limit, and the last slice may be partially filled.

            If you later want to avoid partial leading-edge slices, you can
            adjust end_ms downward to the last "full" span boundary, e.g.:

                total_ms   = end_ms - start_ms + 1
                span       = interval_ms * limit
                full_spans = total_ms // span
                if full_spans > 0:
                    end_ms = start_ms + full_spans * span - 1

    Args:
        symbol:
            Trading pair symbol, e.g. "BTCUSDT".

        interval:
            "1s" or "1m" in the current design.

        limit:
            Max candles per slice (1000 in our design).

        now_ms:
            Optional "now" timestamp for reproducibility/testing. If None,
            the current system time is used.

        timeout_sec:
            HTTP request timeout for probe calls.

    Returns:
        (start_ms, end_ms) in milliseconds.

    Raises:
        RuntimeError if no data could be found for the pair+interval.
    """
    if now_ms is None:
        now_ms = _now_ms()

    interval_ms = interval_to_ms(interval)

    earliest = probe_earliest_open_time(
        symbol=symbol,
        interval=interval,
        timeout_sec=timeout_sec,
    )
    if earliest is None:
        # No data at all (or unreachable). For now, treat as fatal and let
        # the worker crash or be supervised; you can soften this later.
        raise RuntimeError(f"No kline data found for {symbol} @ {interval}")

    latest = probe_latest_open_time(
        symbol=symbol,
        interval=interval,
        timeout_sec=timeout_sec,
        now_ms=now_ms,
    )
    if latest is None:
        # Extremely unlikely if earliest was not None, but guard anyway.
        latest = now_ms

    start_ms = earliest
    end_ms = latest

    # Optional "anchor back to last full slice" logic, left as a simple
    # comment for now. Uncomment/adapt when you decide on the exact rule.
    #
    # span = interval_ms * limit
    # if end_ms > start_ms:
    #     total_ms = end_ms - start_ms + 1
    #     full_spans = total_ms // span
    #     if full_spans > 0:
    #         end_ms = start_ms + full_spans * span - 1

    return start_ms, end_ms
