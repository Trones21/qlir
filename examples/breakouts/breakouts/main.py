from pandas import DataFrame
from qlir.data.sources.drift.fetch import get_candles
from breakouts.detect import tag_breakouts_simple
from qlir.utils.logdf import logdf
from qlir.io.writer import write_csv
from qlir.io.reader import read_csv


## This is an old example, good for referencing, but I'll need to update the fetchers 

# def run():
#     try:
#         existing_candles = read_csv("./data/5m-sol.csv")
#         candles = fetch_drift_candles_update_from_df(existing_candles, "SOL-PERP", 5)
#     except:
#         print("Couldnt find existing csv or append only failed, grabbing all candles")
#         candles = fetch_drift_candles_all("SOL-PERP", 5)
#         write_csv(candles, "./data/5m-sol.csv")

#     print("df len", len(candles))
#     df = tag_breakouts_simple(candles, price_col="close", lookback=20, min_move=0.03)
#     is_true = df.loc[(df["breakout_up"]) | (df["breakout_down"])]
#     logdf(is_true)

# if __name__ == "__main__":
#     run()
