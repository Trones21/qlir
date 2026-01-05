AG_ROOT=~/qlir_data/binance/klines/agg
SYMBOL=BTCUSDT
INTERVAL=1m
LIMIT=1000

TARGET="$AG_ROOT/$SYMBOL/$INTERVAL/$LIMIT"

pkill -f ag-server || true
rm -rf "$TARGET"
poetry run ag-server --symbol "$SYMBOL" --interval "$INTERVAL"