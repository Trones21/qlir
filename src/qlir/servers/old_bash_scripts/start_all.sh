#!/usr/bin/env bash
set -euo pipefail

# ========================
# Required environment
# ========================
missing=()

[[ -z "${TELEGRAM_BOT_TOKEN:-}" ]] && missing+=("TELEGRAM_BOT_TOKEN")
[[ -z "${TELEGRAM_CHAT_ID:-}"  ]] && missing+=("TELEGRAM_CHAT_ID")

if (( ${#missing[@]} > 0 )); then
  echo "[ERROR] Missing required environment variables:"
  for v in "${missing[@]}"; do
    echo "  - $v"
  done
  echo
  echo "Export them before running this script."
  exit 1
fi

# ========================
# Global config
# ========================
EXCHANGE="binance"
STREAM="klines"
SYMBOL="SOLUSDT"
INTERVAL="1m"
LIMIT=1000
PARQUET_BATCH_SIZE_IN_NUM_REQUESTS=500

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# ========================
# Haz-implemented guards
# ========================
if [[ "$EXCHANGE" != "binance" ]]; then
  echo "[ERROR] EXCHANGE='$EXCHANGE' not implemented (only 'binance' supported)"
  exit 1
fi

if [[ "$STREAM" != "klines" ]]; then
  echo "[ERROR] STREAM='$STREAM' not implemented (only 'klines' supported)"
  exit 1
fi

# ========================
# Load Helper
# ========================
source ./start_tmux_func.sh

# ========================
# Commands
# ========================

DATA_SERVER_CMD=(
  poetry run data_server
  --exchange "$EXCHANGE"
  --stream "$STREAM"
  --symbol "$SYMBOL"
  --interval "$INTERVAL"
  --limit "$LIMIT"
)

AGG_CMD=(
  poetry run agg_server
  #--exchange "$EXCHANGE"
  --symbol "$SYMBOL"
  --interval "$INTERVAL"
  --batch-slices "$PARQUET_BATCH_SIZE_IN_NUM_REQUESTS"
)

ANALYSIS_CMD=(
  poetry run analysis_server
  --exchange "$EXCHANGE"
  --symbol "$SYMBOL"
  --interval "$INTERVAL"
)

NOTIFY_CMD=(
  poetry run notifications_server
)

# ========================
# Launch
# ========================
# echo "${AGG_CMD[@]}"
# echo $ANALYSIS_CMD
# echo $DATA_SERVER_CMD
# echo $NOTIFY_CMD

start_tmux agg "${AGG_CMD[@]}"
start_tmux analysis "${ANALYSIS_CMD[@]}"

tmux list-sessions

# start_tmux data    "${DATA_SERVER_CMD[@]}"
# 
# start_tmux notify    "${NOTIFY_CMD[@]}"

# echo "Please verify that the servers actually started (look in logs folder or attach to session). Common issue: Command not found (poetry/pyproject.toml error)" 