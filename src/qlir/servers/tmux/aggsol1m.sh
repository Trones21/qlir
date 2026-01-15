tmux new-session -d -s qlir_agg 'poetry run agg_server --endpoint klines --symbol SOLUSDT --interval 1m --limit 1000 --batch-slices 1000 2>&1 | tee -a logs/agg_server.log'
