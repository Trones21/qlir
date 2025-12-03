import time

import pytest
import logging
log = logging.getLogger(__name__)
from qlir.data.sources.binance.endpoints.klines.time_range import compute_time_range

# This is a LIVE integration test against Binance.
# You probably want to mark it so it can be skipped by default in CI:
pytestmark = pytest.mark.live_binance


# ---------------------------------------------------------------------------
# Configuration for this test
# ---------------------------------------------------------------------------

# Use a fixed "now" timestamp for reproducibility.
# 1) Set this to int(time.time() * 1000) at the moment you decide.
# 2) Run compute_time_range(...) manually (e.g. in a REPL) with that same now_ms.
# 3) Paste the resulting (start_ms, end_ms) into EXPECTED_RANGES below.
NOW_MS_FOR_TEST = 0  # <-- TODO: set this once, e.g. int(time.time() * 1000)

# Fill these in after you manually probe with the same NOW_MS_FOR_TEST.
# Example process (in a REPL / notebook):
#
#   from qlir.data.sources.binance.endpoints.klines.time_range import compute_time_range
#   NOW_MS_FOR_TEST = 1733333333000  # choose this and paste above
#   compute_time_range("BTCUSDT", "1m", limit=1000, now_ms=NOW_MS_FOR_TEST)
#   compute_time_range("ETHUSDT", "1m", limit=1000, now_ms=NOW_MS_FOR_TEST)
#
# Then copy the returned tuples into EXPECTED_RANGES.
EXPECTED_RANGES = {
    # "symbol": (expected_start_ms, expected_end_ms)
    "BTCUSDT": (0, 0),  # TODO: replace with real values
    "ETHUSDT": (0, 0),  # TODO: replace with real values
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

    expected = EXPECTED_RANGES.get(symbol)
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
    assert start_ms == expected_start_ms, (
        f"{symbol} {interval}: start_ms mismatch: "
        f"got {start_ms}, expected {expected_start_ms}"
    )
    assert end_ms == expected_end_ms, (
        f"{symbol} {interval}: end_ms mismatch: "
        f"got {end_ms}, expected {expected_end_ms}"
    )
