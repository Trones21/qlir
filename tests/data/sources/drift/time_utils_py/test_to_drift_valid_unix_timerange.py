import time
from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.sources.drift.symbol_map import DriftSymbolMap
from qlir.data.sources.drift.time_utils import to_drift_valid_unix_timerange
from qlir.data.sources.drift.resolution_helpers import timefreq_to_driftres_string
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit

import logging 
log = logging.getLogger(__name__)

def test_to_drift_valid_unix_timerange():
    # Arrange
    instrument = CanonicalInstrument.SOL_PERP
    drift_symbol = DriftSymbolMap.to_venue(instrument)
    res = TimeFreq(count=1, unit=TimeUnit.MINUTE)
    drift_res = timefreq_to_driftres_string(res)

    # Currently leaving from_ts and to_ts params empty, so we will need to set those based on  

    # Act 
    first_unix, final_unix = to_drift_valid_unix_timerange(drift_symbol=drift_symbol, drift_res=drift_res)
    minute_floored_final_unix = final_unix // 60 * 60
    
    # Set the expected first/last ts explicitly 
    expected_first_unix = 1732782960 # for SOL-PERP, Drift first 1min unix timestamp is:  
    expected_final_unix = int(time.time() // 60 * 60) # take the current minute and round down:
    
    # Assert
    assert first_unix == expected_first_unix
    assert minute_floored_final_unix == expected_final_unix