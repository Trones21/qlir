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
INTERVAL="1s"
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
# Helpers
# ========================
start_tmux() {
  local name="$1"
  shift

  if tmux has-session -t "$name" 2>/dev/null; then
    echo "[WARN] tmux session '$name' already exists"
    return
  fi

  tmux new-session -d -s "$name" \
    "$* 2>&1 | tee $LOG_DIR/$name.log"

  echo "[OK] started $name  →  tmux attach -t $name"
}

# ========================
# Commands
# ========================

DATA_SERVER_CMD=(
  poetry run binance-data-server-arg
  --exchange "$EXCHANGE"
  --stream "$STREAM"
  --symbol "$SYMBOL"
  --interval "$INTERVAL"
  --limit "$LIMIT"
)

AGG_CMD=(
  poetry run binance-agg-server-arg
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
  poetry run notification_server
)

# ========================
# Launch
# ========================
start_tmux agg       "${AGG_CMD[@]}"
start_tmux klines    "${KLINES_CMD[@]}"
start_tmux analysis  "${ANALYSIS_CMD[@]}"
start_tmux notify    "${NOTIFY_CMD[@]}"
