import math
import time

import pytest
import logging
log = logging.getLogger(__name__)
from qlir.data.sources.binance.endpoints.klines.time_range import compute_time_range

# ---------------------------------------------------------------------------
# Configuration for this test
# ---------------------------------------------------------------------------

# Current UTC time in milliseconds
now_ms = int(time.time() * 1000)
NOW_MS_FOR_TEST = now_ms

# Lengths of time windows
MS_PER_SECOND = 1000
MS_PER_MINUTE = 60_000

# ---- FLOORING OPERATIONS ----

# Floor to the current UTC minute (in milliseconds)
minute_floor_ms = (now_ms // MS_PER_MINUTE) * MS_PER_MINUTE

# Floor to the current UTC second (in milliseconds)
second_floor_ms = (now_ms // MS_PER_SECOND) * MS_PER_SECOND

# Fill these in after you manually probe with the same NOW_MS_FOR_TEST.
# Example process (in a REPL / notebook):
#
#   from qlir.data.sources.binance.endpoints.klines.time_range import compute_time_range
#   NOW_MS_FOR_TEST = 1733333333000  # choose this and paste above
#   compute_time_range("BTCUSDT", "1m", limit=1000, now_ms=NOW_MS_FOR_TEST)
#   compute_time_range("ETHUSDT", "1m", limit=1000, now_ms=NOW_MS_FOR_TEST)
#
# or just run the test and copy the built urls into a browser to get the values

EXPECTED_RANGES = {
    # "symbolinterval": (expected_start_ms, expected_end_ms)
    "BTCUSDT1m": (1502942400000 , minute_floor_ms),
    "ETHUSDT1m": (1502942400000, minute_floor_ms), 
    "SOLUSDT1m": ( 1597125600000, minute_floor_ms),
    "BTCUSDT1s": (1502942428000, second_floor_ms),
    "ETHUSDT1s": (1502942429000, second_floor_ms), #need to get via the url when internet is back https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1s&limit=1&startTime=0 
    "SOLUSDT1s": (1597125600000, second_floor_ms),
}


# ---------------------------------------------------------------------------
# The actual test
# ---------------------------------------------------------------------------

@pytest.mark.network
@pytest.mark.datasources_binance
@pytest.mark.datasources_binance_klines
@pytest.mark.parametrize("symbol, interval", [
    ("BTCUSDT", "1m"),
    ("ETHUSDT", "1m"),
    ("SOLUSDT", "1m"),
    ("BTCUSDT", "1s"),
    ("ETHUSDT", "1s"),
    ("SOLUSDT", "1s"),
])
def test_compute_time_range_live(symbol: str, interval: str):
    """
    Live test for compute_time_range against Binance.

    This asserts that for a fixed "now_ms", the computed (start_ms, end_ms)
    matches manually verified expected ranges for two different symbols.
    """
    log.info("test compute time range")
    assert NOW_MS_FOR_TEST != 0, (
        "NOW_MS_FOR_TEST is still 0. Set it to a fixed int(time.time() * 1000) "
        "and fill EXPECTED_RANGES before running this test."
    )

    expected = EXPECTED_RANGES.get(symbol+interval)
    assert expected is not None, f"No expected range configured for {symbol}"

    expected_start_ms, expected_end_ms = expected

    start_ms, end_ms = compute_time_range(
        symbol=symbol,
        interval=interval,
        limit=1000,
        now_ms=NOW_MS_FOR_TEST,
    )

    # Exact equality since you will paste the values that compute_time_range
    # produced for this exact NOW_MS_FOR_TEST.
    # log.info(f"start_ms: {start_ms}, expected_start_ms: {expected_start_ms}")
    assert start_ms == expected_start_ms, (
        f"{symbol} {interval}: start_ms mismatch: "
        f"got {start_ms}, expected {expected_start_ms}"
    )
    # log.info(f"end_ms: {end_ms}, expected_end_ms: {expected_end_ms}")
    assert end_ms == expected_end_ms, (
        f"{symbol} {interval}: end_ms mismatch: "
        f"got {end_ms}, expected {expected_end_ms}"
    )
