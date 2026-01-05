QLIR_MANIFEST_LOG=1
QLIR_REFRESH_ON_METADATA_SCHEMA_MISMATCH=1


# How to view the manifest deltalog logs
# Open another terminal and run:
# tail -f /path/to/data_root/binance/.../logs/manifest_aggregator.log 
# e.g. tail -f /home/tjr/qlir_data/binance/klines/raw/BTCUSDT/1m/limit=1000/logs/

# Run the Data Server
poetry run binance-data-server-arg --endpoint klines --symbol BTCUSDT --interval "1s" --log-profile "qlir-debug" 

# 10m test run
# 