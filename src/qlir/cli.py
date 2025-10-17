import argparse, sys
from .data.csv import load_ohlcv_from_csv
from .data.drift import fetch_drift_candles

def main():
    p = argparse.ArgumentParser(prog="qlir")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_csv = sub.add_parser("csv", help="Load and echo normalized CSV")
    p_csv.add_argument("path")

    p_fetch = sub.add_parser("fetch", help="Fetch Drift candles")
    p_fetch.add_argument("--symbol", default="SOL-PERP")
    p_fetch.add_argument("--res", default="1")
    p_fetch.add_argument("--limit", type=int)

    args = p.parse_args()
    if args.cmd == "csv":
        df = load_ohlcv_from_csv(args.path)
        print(df.head(20).to_string(index=False))
    elif args.cmd == "fetch":
        df = fetch_drift_candles(symbol=args.symbol, resolution=args.res, limit=args.limit)
        print(df.tail().to_string(index=False))
    else:
        p.print_help(); sys.exit(2)

if __name__ == "__main__":
    main()
