AG_ROOT=~/qlir_data/binance/klines/agg
SYMBOL=SOLUSDT
INTERVAL=1s
LIMIT=1000

TARGET="$AG_ROOT/$SYMBOL/$INTERVAL/limit=$LIMIT"

pkill -f agg_server || true
echo "Any existing agg_server procs killed"

rm -rfv "$TARGET"
echo "Cleaned dir: $TARGET"

poetry run agg_server --endpoint "klines" --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --batch-slices 100
