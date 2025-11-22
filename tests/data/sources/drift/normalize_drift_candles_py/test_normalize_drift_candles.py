import time
import pytest
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.sources.drift.normalize_drift_candles import normalize_drift_fills_candles
from qlir.data.sources.drift.symbol_map import DriftSymbolMap
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
import logging 
log = logging.getLogger(__name__)

def test_normalize_drift_candles_daily_res(drift_candles_df):
    # Arrange
    df = drift_candles_df 
    resolution = "D"
    
    # Act 
    normalized = normalize_drift_fills_candles(df, resolution=resolution, include_partial=False)

    # Assert
    log.info(normalized)