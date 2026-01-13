

# Tail in another terminal 
# tail -f /home/tjr/qlir_data/binance/klines/raw/SOLUSDT/1m/limit=1000/logs/

poetry run binance-data-server-arg --endpoint klines --symbol SOLUSDT --interval "1m" --log-profile "qlir-debug"