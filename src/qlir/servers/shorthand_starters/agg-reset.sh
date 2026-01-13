AG_ROOT=~/qlir_data/binance/klines/agg
SYMBOL=SOLUSDT
INTERVAL=1s
LIMIT=1000

TARGET="$AG_ROOT/$SYMBOL/$INTERVAL/limit=$LIMIT"

pkill -f binance-agg-server-arg || true
echo "Any existing binance-agg-server-arg procs killed"

rm -rfv "$TARGET"
echo "Cleaned dir: $TARGET"

poetry run binance-agg-server-arg --endpoint "klines" --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --batch-slices 100
