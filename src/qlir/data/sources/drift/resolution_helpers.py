#### ToDo:
# These are a work in progress, we currently have resolution mapping stored all over the place
# constants.DRIFT_ALLOWED_RESOLUTIONS
# DriftCandleSpec has some values in registry.py
# GetMarketSymbolCandlesResolutionResolution contains strings from the drift sdk
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
from drift_data_api_client.models.get_market_symbol_candles_resolution_resolution import GetMarketSymbolCandlesResolutionResolution


def driftres_typed_to_string(drift_res_typed: GetMarketSymbolCandlesResolutionResolution):
    return drift_res_typed.__str__()

def timefreq_to_driftres_typed(timefreq: TimeFreq) -> GetMarketSymbolCandlesResolutionResolution:
    if timefreq.count != 1:
        "QLIR currently only supports 1min, 1hour, 1day timefreq pulls from drift"
    
    if timefreq.unit is TimeUnit.MINUTE:
        return GetMarketSymbolCandlesResolutionResolution.VALUE_0
    
    if timefreq.unit is TimeUnit.DAY:
        return GetMarketSymbolCandlesResolutionResolution.D
    
    raise( ValueError("QLIR currently only supports 1min, 1day timefreq pulls from drift") )

def timefreq_to_driftres_string(timefreq: TimeFreq) -> str:
    if timefreq.count != 1:
        "QLIR currently only supports 1min, 1hour, 1day timefreq pulls from drift"
    
    if timefreq.unit is TimeUnit.MINUTE:
        return GetMarketSymbolCandlesResolutionResolution.VALUE_0.value
    
    if timefreq.unit is TimeUnit.DAY:
        return GetMarketSymbolCandlesResolutionResolution.D.value
    
    raise( ValueError("QLIR currently only supports 1min, 1day timefreq pulls from drift") )

