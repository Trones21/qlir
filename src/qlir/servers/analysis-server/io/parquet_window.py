


    # 
    # df = union_file_datasets(parquet_chunks_dir)
    # log.info(df.columns)
from qlir.io.reader import load_latest_parquet_window


def load_parquet_window():
    parquet_chunks_dir = AGG_DIR_PATH
    df = load_latest_parquet_window(parquet_chunks_dir, last_n_files=5)
    return df