from pathlib import Path
import pandas as pd

from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.core.paths import candles_path
from qlir.io.writer import _prep_path, write, write_dataset_meta
from qlir.time.timefreq import TimeFreq


def writedf_and_metadata(df: pd.DataFrame, base_resolution: TimeFreq, symbol: CanonicalInstrument, save_dir_override: Path | None = None):
    canonical_resolution_str = base_resolution.to_canonical_resolution_str()
    canonical_instr = symbol.value

    if save_dir_override is not None:
        dirpath = _prep_path(save_dir_override)
    else:
        dirpath = None # Normal case - candles_path will handle this

    dataset_uri = candles_path(user_root=dirpath, instrument_id=canonical_instr, resolution=canonical_resolution_str, datasource="DRIFT")
        
    write(df, dataset_uri)
    write_dataset_meta(dataset_uri, instrument_id=symbol.value, resolution=canonical_resolution_str)
    
    return df