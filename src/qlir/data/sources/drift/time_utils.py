from datetime import datetime, timezone
from typing import Optional

import pandas as _pd
import requests

from qlir.data.sources.drift.discovery import discover_earliest_candle_start


def _unix_s(x: _pd.Timestamp | int | float | None) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, _pd.Timestamp):
        return int(x.tz_convert("UTC").timestamp())
    x = float(x)
    return int(x / 1000.0) if x > 1_000_000_000_000 else int(x)


def to_drift_valid_unix_timerange(drift_symbol: str, drift_res: str , from_ts: datetime | None = None, to_ts: datetime | None = None):

    # Handle to_ts (using current time if None)
    intended_final_unix = datetime.now(timezone.utc) if to_ts is None else to_ts
    intended_final_unix = int(intended_final_unix.timestamp())
    # -------

    # Handle from_ts:
    catalog_min_unix = 1668470400 # This is a drift limit, later we may want to to refactor discover_earliest_candle_start to work with multiple venues
    timeout = 15.0  # using a reasonable value 
    session = requests.Session()
    # if None, discover earliest candle start
    if from_ts is None:
        discovered = discover_earliest_candle_start(
            session=session,
            symbol=drift_symbol,
            resolution=drift_res,
            end_bound_unix=intended_final_unix,
            catalog_min_unix=catalog_min_unix,
            timeout=timeout,
            include_partial=False,
        )
        if discovered is None:
            raise(ValueError("Error discovering earliest drift candle") )
        
        intended_first_unix = discovered
    else:
        # else use passed value
        intended_first_unix = int(from_ts.timestamp())
    # -------

    return intended_first_unix, intended_final_unix


