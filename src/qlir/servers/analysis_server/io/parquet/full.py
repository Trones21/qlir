



import pandas as _pd
from qlir.io.union_files import union_file_datasets
from qlir.servers.analysis_server.etl.pipelines.first_pipeline import clean_data


def load_parquet(agg_dir_path) -> _pd.DataFrame:
    
    return union_file_datasets(agg_dir_path)
