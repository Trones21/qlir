from drift_data_api_client import Client
from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
from qlir.data.normalize import normalize_candles
from qlir.utils.logdf import logdf
import time
from datetime import datetime, timezone
import math
import pandas as pd
import logging
log = logging.getLogger(__name__)

def test_get_funding_rates():
    client = Client("https://data.api.drift.trade")
    resp = get_market_symbol_funding_rates.sync("SOL-PERP", client=client)
    if resp is not None:
        log.info(type(resp))
        log.info(resp.records)
        assert True
    else:
        assert False

# start_ts param returns a record set where start_ts is the largest value (not the smallest, as the name implies)
# therefore if you pass a start_ts of Oct 1st at 4am (in unix epoch seconds int), then that'll be the last record retrieved. Your first record returned 
# is determined by the limit param (or default of 100)
# Think of it as: 
# select * from {res}_candles WHERE timestamp < {start_ts} ORDER BY timstamp DESC LIMIT {limit}

# end_ts can be even more confusing, because the limit param is applied first. Your true largest value is actually the current time.
# So you might put an end_ts of Oct 1st at 4am, but lets say you have a limit param of 50, and current-50 == Oct 3rd at 6pm
# in this case the limit param renders the end_ts irrelevant
# Think of it as: 
# select * from {res}_candles WHERE timestamp > {end_ts} ORDER BY timstamp DESC LIMIT {limit}
     
def test_loop_candles():
    client = Client("https://data.api.drift.trade")
    # Get hourly candles for last 72 hours, get 20 candles per fetch

    start_ts = math.floor(time.time() - (86400 * 3))
    log.info("Anchor time: %s",  datetime.fromtimestamp(start_ts, timezone.utc))
    
    pages: list[pd.DataFrame] = [] 
    #while start_ts < math.floor(time.time()):
    temp = 0 
    while temp < 3:
        log.info(f"start_ts: {start_ts} current_ts: {math.floor(time.time())}")
        resp = get_market_symbol_candles_resolution.sync("SOL-PERP", client=client, resolution=GetMarketSymbolCandlesResolutionResolution("60"), limit=20, start_ts=start_ts)
        if resp.records:
            #log.info(type(resp.records))
            resp_as_dict = resp.to_dict()
            # log.info(resp_as_dict["records"])
            page = pd.DataFrame(resp_as_dict["records"])
            #logdf(page)
            #log.info("len : %s", len(page))
            clean = normalize_candles(page, venue="drift", resolution="60", keep_ts_start_unix=True, include_partial=False)
            sorted = clean.sort_values("tz_start").reset_index(drop=True)
            pages.append(sorted)
            start_ts = int(sorted["ts_start_unix"].max())
            first_row = sorted.iloc[0]
            last_row =  sorted.iloc[-1]
            log.info(f"First row unix:{first_row['ts_start_unix']} tz_start: {first_row['tz_start']}")
            log.info(f"Last  row unix:{last_row['ts_start_unix']} tz_start: {last_row['tz_start']}")
            #logdf(sorted, 100)
            temp += 1
            log.info("Expected next start_ts at least: %s", start_ts)

    # for page in pages:
    #     logdf(page)