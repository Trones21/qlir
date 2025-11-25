from pathlib import Path
import pandas as pd

from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.core.paths import candles_path
from qlir.io.writer import _prep_path, write, write_dataset_meta
from qlir.time.timefreq import TimeFreq
import logging
log = logging.getLogger(__name__)

def writedf_and_metadata(df: pd.DataFrame, base_resolution: TimeFreq, symbol: CanonicalInstrument, dir_suffix_str: str | None = None):
    canonical_instr = symbol.value
    canonical_resolution_str = base_resolution.to_canonical_resolution_str()
    
    if dir_suffix_str is None:
        dir_suffix_str = ""
    dataset_uri = candles_path(instrument_id=canonical_instr, resolution=base_resolution, datasource="DRIFT", dir_suffix_str=dir_suffix_str)
    
    write(df, dataset_uri)
    write_dataset_meta(dataset_uri, instrument_id=symbol.value, resolution=canonical_resolution_str)
    
    return df