#!/usr/bin/env bash
# Start a 1s data_server for SOL, BTC, and ETH, each in its own tmux session.
# (Each data_server is a long-running daemon, so they must run as separate
#  sessions rather than sequential blocking calls.)

mkdir -p logs

tmux new-session -d -s qlir_data_sol1s 'poetry run data_server --endpoint klines --symbol SOLUSDT --interval 1s --log-profile qlir-info 2>&1 | tee -a logs/data_server_sol1s.log'
tmux new-session -d -s qlir_data_btc1s 'poetry run data_server --endpoint klines --symbol BTCUSDT --interval 1s --log-profile qlir-info 2>&1 | tee -a logs/data_server_btc1s.log'
tmux new-session -d -s qlir_data_eth1s 'poetry run data_server --endpoint klines --symbol ETHUSDT --interval 1s --log-profile qlir-info 2>&1 | tee -a logs/data_server_eth1s.log'

tmux list-sessions
