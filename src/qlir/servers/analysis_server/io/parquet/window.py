    # 
    # df = union_file_datasets(parquet_chunks_dir)
    # log.info(df.columns)
from qlir.io.reader import load_latest_parquet_window


def load_parquet_window(agg_dir_path, last_n_files: int):

    df = load_latest_parquet_window(directory=agg_dir_path, window_size=last_n_files)

    return df