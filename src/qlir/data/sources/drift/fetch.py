from __future__ import annotations
from typing import Any, Dict, Optional
import time
import requests
import pandas as pd
import logging

from qlir.data.core.infer import infer_dataset_identity

from qlir.data.sources.drift.symbol_map import DriftSymbolMap
from qlir.data.sources.drift.time_utils import timefreq_to_driftres_typed, to_drift_valid_unix_timerange
log = logging.getLogger(__name__)
from qlir.data.core.instruments import CanonicalInstrument
from qlir.time.timefreq import TimeFreq
from qlir.utils.logdf import logdf
from pathlib import Path
from drift_data_api_client import Client
from drift_data_api_client.api.market import get_market_symbol_funding_rates, get_market_symbol_candles_resolution
from drift_data_api_client.models import get_market_symbol_candles_resolution_resolution, get_market_symbol_candles_resolution_response_200
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200RecordsItem
from drift_data_api_client.models import GetMarketSymbolCandlesResolutionResponse200
from qlir.data.candle_quality import validate_candles
from qlir.data.sources.drift.normalize_drift_candles import normalize_drift_candles
from qlir.io.checkpoint import write_checkpoint, FileType
from qlir.io.union_files import union_file_datasets
from qlir.io.reader import read
from qlir.data.sources.drift.constants import DRIFT_BASE_URI, DRIFT_ALLOWED_RESOLUTIONS
from qlir.df.utils import union_and_sort
from datetime import datetime, timezone

from qlir.data.sources.drift.write_wrappers import writedf_and_metadata

import math

def get_candles(symbol: CanonicalInstrument, base_resolution: TimeFreq, from_ts: datetime | None = None, to_ts: datetime | None = None, save_dir_override: Path | None = None, filetype_out: FileType = FileType.PARQUET):
    
    drift_symbol = DriftSymbolMap.to_venue(symbol)
    client = Client(DRIFT_BASE_URI)
    drift_res = timefreq_to_driftres_typed(base_resolution)
    
    intended_first_unix, intended_final_unix = to_drift_valid_unix_timerange(drift_symbol, drift_res)
    
    temp_folder = f"tmp/{drift_symbol}_{drift_res}/"

    next_call_unix = intended_final_unix # seed for FIRST call only

    log.info("Paginiating backwards from: %s", datetime.fromtimestamp(intended_final_unix, timezone.utc))
    
    pages: list[pd.DataFrame] = [] 
    earliest_got_ts = int(math.inf)
    while earliest_got_ts > intended_first_unix:
        #log.info(f"earliest_got_ts: {earliest_got_ts} > anchor_ts: {math.floor(time.time())}")
        resp = get_market_symbol_candles_resolution.sync(drift_symbol, client=client, resolution=GetMarketSymbolCandlesResolutionResolution(drift_res), limit=20, start_ts=next_call_unix-1)
        
        if resp is None:
            log.info("Empty Response, trying again")
            continue

        if resp.records:
            resp_as_dict = resp.to_dict()
            page = pd.DataFrame(resp_as_dict["records"])
            clean = normalize_drift_candles(page, resolution=drift_res, keep_ts_start_unix=True, include_partial=False)
            sorted = clean.sort_values("tz_start").reset_index(drop=True)
            pages.append(sorted)
            
            # currently going to paginate backward from final_ts
            first_row = sorted.iloc[0]
            last_row =  sorted.iloc[-1]
            log.info(f"Retrieved bars starting with: {first_row['tz_start']} to {last_row['tz_start']}")
            # logdf(sorted, 25)
            earliest_got_ts = first_row['ts_start_unix']
            next_call_unix = earliest_got_ts - 1 # remove 1 second to avoid duplicating a row
            if len(pages) > 2:
                data = (
                    pd.concat(pages, ignore_index=True)
                    .sort_values("tz_start")
                    .drop_duplicates(subset=["tz_start"], keep="last")
                    .reset_index(drop=True)
                )
                write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname=temp_folder)
                pages = []

    if pages:
        data = (
            pd.concat(pages, ignore_index=True)
            .sort_values("tz_start")
            .drop_duplicates(subset=["tz_start"], keep="last")
            .reset_index(drop=True)
        )
        write_checkpoint(data, file_type=FileType.CSV, static_part_of_pathname=temp_folder)

    single_df = union_file_datasets("tmp") #note that this doesnt use read_candles but just read, could switch over later if we feel the need
    clean_df, dq_report = validate_candles(single_df, base_resolution)

    # We should verify the range 
    if dq_report.n_gaps > 0:
        log.warning("Candle Data has gaps, saving to file anyway, please run fill candle gaps") 
    
    log.info("First Candle: %s", dq_report.first_ts)
    log.info("Last Candle: %s", dq_report.final_ts)

    writedf_and_metadata(clean_df, base_resolution, symbol, save_dir_override)
    
    return clean_df


## This was written 
def add_new_candles_to_dataset(existing_file: str, symbol_override: str | None = None):
    dataset_uri = Path(existing_file)
    existing_df = read(dataset_uri)
    
    log.info("Currently using infer_freq only, not a full data quality check")

    dataset_identity = infer_dataset_identity(dataset_uri)

    current_last_candle = existing_df["tz_start"].max()
    drift_symbol = dataset_identity["upstream_symbol"]
    resolution_str = dataset_identity["resolution"]

    if any(x is None for x in [drift_symbol, resolution_str]):
        raise ValueError(f"Drift symbol and resolution_str cannot be None: drift_symbol: {type(drift_symbol)}  resolution_str: {type(resolution_str)}")

    if drift_symbol is None and symbol_override is None:
        raise Exception("Symbol cannot be inferred from existing_file or associated .meta.json and no symbol_override was passed, we therefore do not know which symbol to retrieve data for")
    if drift_symbol is None:
        effective_symbol = symbol_override
    else:
        effective_symbol = drift_symbol

    # Cast.convert to types that get_candles func needs 
    canoncial_instr = DriftSymbolMap.to_canonical(effective_symbol)
    timefreq = TimeFreq.from_canonical_resolution_str(resolution_str) #type: ignore - pylance just isnt very good at understanding if any(x is None for x in [drift_symbol, resolution_str]):
   
    log.info(f"Getting new {resolution_str} {effective_symbol} candles since {current_last_candle}")
    
    new_candles_df = get_candles(canoncial_instr, base_resolution=timefreq, from_ts=current_last_candle )
    full_df = union_and_sort([existing_df, new_candles_df], sort_by=["tz_start"])
    
    writedf_and_metadata(full_df, symbol=canoncial_instr, base_resolution=timefreq)

    return full_df


def get_all_candles(symbol: CanonicalInstrument,  base_resolution: TimeFreq): 
    return get_candles(symbol, base_resolution)