import pathlib
import pandas as pd
from .helpers import detect_homogeneous_filetype
from .filetype import FileType
from .reader import read
from pathlib import Path
import pyarrow.dataset as ds
import logging
log = logging.getLogger(__name__)

def union_file_datasets(dir_path: str | Path) -> pd.DataFrame: # type: ignore
    log.info(dir_path)
    dir = Path(dir_path)
    log.info(f"Unioning files in {dir_path}")
    #check for homogenous extensions
    filetype = detect_homogeneous_filetype(dir_path)
    
    #if heterogenous extensions, then warn, but proceed file by file 
    if filetype is None:
        log.info("Multiple file types found in: %s", dir_path)
        dfs: list[pd.DataFrame] = []
        for file in dir.iterdir():
            dfs.append(read(file))
        data = pd.concat(dfs, ignore_index=True)
        return data
    
    # Process Homogeneous 
    if filetype:
        # Fast path for parquet files 
        if filetype == FileType.PARQUET:
            dataset = ds.dataset(dir_path, format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            return df
        else:
            # Going for a list of dataframes rather than straight append for debuggability purposes
            dfs: list[pd.DataFrame] = []
            for file in dir.iterdir():
                dfs.append(read(file))
            data = pd.concat(dfs, ignore_index=True)
            return data

        
