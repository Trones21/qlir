
from datetime import datetime, timezone
import math
import time
from drift_data_api_client import Client
from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
import pytest
import requests
from qlir.data.drift import discover_earliest_candle_start
import logging
log = logging.getLogger(__name__)


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




def test_future_date():
    '''See what happens when we give drift a date in the future as the start ts'''
    client = Client("https://data.api.drift.trade")
    # Get 100 hourly candles with a anchor (last candle) of 48 hours in the future 
    # Currently its a 400 res 
     
    anchor_ts = math.floor(time.time() + (86400 * 2))
    log.info("Anchor time: %s",  datetime.fromtimestamp(anchor_ts, timezone.utc))

    res = get_market_symbol_candles_resolution.sync_detailed("SOL-PERP", client=client, resolution=GetMarketSymbolCandlesResolutionResolution("60"), start_ts=anchor_ts)
    
    if res.status_code != 400:
        pytest.fail(f"Passed a futue date and expected a 400 res, but {res.status_code} was returned")