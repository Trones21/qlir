
source ~/set_telegram_env_vars.sh

QLIR_MANIFEST_LOG=1
QLIR_ALERTS_DIR=~/alerts
mkdir -p logs

tmux new-session -d -s qlir_data 'poetry run data_server --endpoint klines --symbol SOLUSDT --interval 1m --limit 1000 2>&1 | tee -a logs/data_server.log'
tmux new-session -d -s qlir_agg 'poetry run agg_server --endpoint klines --symbol SOLUSDT --interval 1m --limit 1000 --batch-slices 1000 2>&1 | tee -a logs/agg_server.log'
tmux new-session -d -s qlir_analysis 'poetry run analysis_server 2>&1 | tee -a logs/analysis_server.log'
tmux new-session -d -s qlir_notify 'poetry run notifications_server 2>&1 | tee -a logs/notifications_server.log'

tmux list-sessions
