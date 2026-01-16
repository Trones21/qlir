# from drift_data_api_client import Client
# from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
# from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
# from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
# from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
# from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
# import pytest
# from qlir.time.timefreq import TimeFreq, TimeUnit
# from qlir.data.quality.candles.candles import validate_candles
# from qlir.data.normalize import normalize_candles
# from qlir.logging.logdf import logdf
# from qlir.io.checkpoint import write_checkpoint, FileType
# from qlir.io.union_files import union_file_datasets
# import time
# from datetime import datetime, timezone
# import math
# import pandas as _pd
# import logging
# log = logging.getLogger(__name__)

# @pytest.mark.skip(reason="Deprecated")
# def test_get_funding_rates():
#     client = Client("https://data.api.drift.trade")
#     resp = get_market_symbol_funding_rates.sync("SOL-PERP", client=client)
#     if resp is not None:
#         log.info(type(resp))
#         log.info(resp.records)
#         assert True
#     else:
#         assert False

# # start_ts param returns a record set where start_ts is the largest value (not the smallest, as the name implies)
# # therefore if you pass a start_ts of Oct 1st at 4am (in unix epoch seconds int), then that'll be the last record retrieved. Your first record returned 
# # is determined by the limit param (or default of 100)
# # Think of it as: 
# # select * from {res}_candles WHERE timestamp < {start_ts} ORDER BY timstamp DESC LIMIT {limit}

# # end_ts can be even more confusing, because the limit param is applied first. Your true largest value is actually the current time.
# # So you might put an end_ts of Oct 1st at 4am, but lets say you have a limit param of 50, and current-50 == Oct 3rd at 6pm
# # in this case the limit param renders the end_ts irrelevant
# # Think of it as: 
# # select * from {res}_candles WHERE timestamp > {end_ts} ORDER BY timstamp DESC LIMIT {limit}

# @pytest.mark.skip(reason="Deprecated")     
# def test_loop_candles():
#     client = Client("https://data.api.drift.trade")
#     # Get hourly candles for last 72 hours, get 20 candles per fetch

#     anchor_ts = math.floor(time.time() - (86400 * 6))
#     final_ts = math.floor(time.time()) #flooring for the apis sake
#     temp_folder = "tmp/SOL_1hr_partial"
#     symbol = "SOL-PERP"
#     resolution = "60"
    
#     next_call_ts = final_ts # seed for FIRST call only

#     log.info("Anchor time: %s",  datetime.fromtimestamp(anchor_ts, timezone.utc))
    
#     pages: list[_pd.DataFrame] = [] 
#     earliest_got_ts = math.inf
#     while earliest_got_ts > anchor_ts:
#         #log.info(f"earliest_got_ts: {earliest_got_ts} > anchor_ts: {math.floor(time.time())}")
#         resp = get_market_symbol_candles_resolution.sync(symbol, client=client, resolution=GetMarketSymbolCandlesResolutionResolution(resolution), limit=20, start_ts=next_call_ts-1)
#         if resp.records:
#             resp_as_dict = resp.to_dict()
#             page = _pd.DataFrame(resp_as_dict["records"])
#             clean = normalize_candles(page, venue="drift", resolution="60", keep_ts_start_unix=True, include_partial=False)
#             sorted = clean.sort_values("tz_start").reset_index(drop=True)
#             pages.append(sorted)
            
#             # currently going to paginate backward from final_ts
#             first_row = sorted.iloc[0]
#             last_row =  sorted.iloc[-1]
#             log.info(f"Retrieved bars starting with: {first_row['tz_start']} to {last_row['tz_start']}")
#             # logdf(sorted, 25)
#             earliest_got_ts = first_row['ts_start_unix']
#             next_call_ts = earliest_got_ts - 1 # remove 1 second to avoid duplicating a row
#             if len(pages) > 2:
#                 data = (
#                     _pd.concat(pages, ignore_index=True)
#                     .sort_values("tz_start")
#                     .drop_duplicates(subset=["tz_start"], keep="last")
#                     .reset_index(drop=True)
#                 )
#                 write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname="")
#                 pages = []

#     if pages:
#         data = (
#             _pd.concat(pages, ignore_index=True)
#             .sort_values("tz_start")
#             .drop_duplicates(subset=["tz_start"], keep="last")
#             .reset_index(drop=True)
#         )
#         write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname="tmp/SOL_1hr_partial_last")

#     single_df = union_file_datasets("tmp") #note that this doesnt use read_candles but just read, could switch over later if we feel the need
#     validate_candles(single_df, TimeFreq(count=1, unit=TimeUnit.HOUR))

