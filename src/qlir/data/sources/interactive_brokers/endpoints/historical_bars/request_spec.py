"""
Build IBKR reqHistoricalData parameters from a SliceKey.

Binance fetches a window as startTime + limit; IBKR fetches it as endDateTime + duration
(walking backward). Same logical window, different request shape. This is the translation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math

from qlir.data.sources.common.slices.slice_key import SliceKey


@dataclass(frozen=True)
class HistoricalRequestSpec:
    end_dt: datetime          # timezone-aware UTC; IBKR walks backward from here
    duration_str: str         # e.g. "60000 S" or "2 D"
    bar_size_setting: str     # IBKR barSizeSetting, e.g. "1 min"
    what_to_show: str
    use_rth: int


def _duration_str(span_ms: int) -> str:
    """IBKR duration: seconds up to 86400, otherwise whole days."""
    seconds = max(1, int(math.ceil(span_ms / 1000.0)))
    if seconds <= 86_400:
        return f"{seconds} S"
    days = int(math.ceil(seconds / 86_400))
    return f"{days} D"


def build_request_spec(
    slice_key: SliceKey,
    *,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: int,
) -> HistoricalRequestSpec:
    # end_ms is the inclusive end of the window; IBKR endDateTime is exclusive-ish, so we
    # anchor one ms past the window end.
    end_dt = datetime.fromtimestamp((slice_key.end_ms + 1) / 1000.0, tz=timezone.utc)
    span_ms = slice_key.end_ms - slice_key.start_ms + 1
    return HistoricalRequestSpec(
        end_dt=end_dt,
        duration_str=_duration_str(span_ms),
        bar_size_setting=bar_size_setting,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
