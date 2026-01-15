import argparse


def main():
    p = argparse.ArgumentParser(prog="qlir")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_csv = sub.add_parser("csv", help="Load and echo normalized CSV")
    p_csv.add_argument("path")

    p_fetch = sub.add_parser("fetch", help="Fetch Drift candles")
    p_fetch.add_argument("--symbol", default="SOL-PERP")
    p_fetch.add_argument("--res", default="1")
    p_fetch.add_argument("--limit", type=int)
    p_fetch.add_argument("--out", help="Optional path to save CSV (e.g. data/sol_perp.csv)")


## Need to rethink the cli, how its going to be used, etc.

#     args = p.parse_args()
#     if args.cmd == "csv":
#         df = load_ohlcv(args.path)
#         print(df.head(20).to_string(index=False))
#     elif args.cmd == "fetch":
#         df = fetch_drift_candles(symbol=args.symbol, resolution=args.res, include_partial=False)
#         df = get_candles(symbol=args.symbol, resolution=args.res, )
#         print(df.tail().to_string(index=False))
#         if args.out:
#             path = write(
#                 df,
#                 args.out,
#                 compression=args.compression if args.out.endswith(".parquet") else None,
#             )
#             print(f"\n✅ Saved {len(df)} rows → {path}")
#     else:
#         p.print_help(); sys.exit(2)

# if __name__ == "__main__":
#     main()
