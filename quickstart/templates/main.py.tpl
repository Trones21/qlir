import pandas as pd


def entrypoint() -> None:
    print("Hello from QLIR quickstart!")
    print("Welcome to your __PROJECT_NAME__ analysis project.")
    # TODO:
    # - remove the early return
    # - wire up your actual QLIR data loader / pipeline
    return

    # Example shape of a typical pipeline (pseudo-code):
    #
    # from qlir.data.sources.loader import get_candles
    # from qlir.data.resampling import resample_ohlcv
    #
    # instrument = "SOL_PERP"
    # datasource = "DRIFT"
    # resolution = "1m"
    #
    # df_raw = get_candles(instrument=instrument, resolution=resolution, datasource=datasource)
    # df = resample_ohlcv(df_raw, target_resolution="5m")
    #
    # update_disk_dataset(df_raw)
    # main(df)


def main(df: pd.DataFrame) -> pd.DataFrame:
    """
    Core analysis body.

    - Add indicators / features / signals
    - Join with other datasets
    - Persist intermediate/final results
    - Optionally produce dataviz-friendly tables, plots, etc.
    """
    # Example:
    # df = add_sma_slope_features(df)
    # df = compute_slope_persistence_stats(df)
    # df.to_parquet("outputs/slope_persistence.parquet")

    return df  # useful for tests / notebooks


if __name__ == "__main__":
    entrypoint()