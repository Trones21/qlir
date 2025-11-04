
from qlir.data.drift import fetch_drift_candles_all
import pytest
import requests
from qlir.data.drift import discover_earliest_candle_start
import logging
log = logging.getLogger(__name__)

@pytest.mark.integration
def test_discover_earliest_candle_start_real():
    """
    Integration test that hits the real Drift API.
    Expected earliest tz_start will be provided manually.
    """
    session = requests.Session()

    symbol = "BTC-PERP"
    res_token = "1"
    catalog_min_unix = 1668470400
    end_bound_unix = 1_800_000_000  # ~2027 for future-proofing

    result = discover_earliest_candle_start(
        session=session,
        symbol=symbol,
        res_token=res_token,
        end_bound_unix=end_bound_unix,
        catalog_min_unix=catalog_min_unix,
        timeout=15.0,
        include_partial=True
    )

    expected_earliest = None
    logging.info(f"\nEarliest tz_start returned: {result}")

    # Placeholder assertion; update once you have expected unix
    assert result is not None
    if expected_earliest is not None:
        assert result == expected_earliest




def test_fetch_loop():
    
