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

  if [ -z "$name" ]; then
    echo "[ERROR] session name required"
    return 1
  fi

  if [ "$#" -eq 0 ]; then
    echo "[ERROR] command required"
    return 1
  fi

  if tmux has-session -t "$name" 2>/dev/null; then
    echo "[WARN] tmux session '$name' already exists"
    return 0
  fi

  tmux new-session -d -s "$name" bash -l -s -- "$@" <<'EOF'
set -euo pipefail

SESSION_NAME="${TMUX_SESSION_NAME:?}"
LOG_FILE="${LOG_DIR:?}/${SESSION_NAME}.log"

echo "[INFO] starting $SESSION_NAME" | tee -a "$LOG_FILE"

"$@" 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}

if [ "$status" -ne 0 ]; then
  echo "[ERROR] $SESSION_NAME exited with status $status" | tee -a "$LOG_FILE"
  tmux wait-for -S "crash:${SESSION_NAME}:${status}"
fi

# optional: keep pane open for inspection
echo "[INFO] $SESSION_NAME finished (status=$status)"
read -r -p "press enter to close session"
EOF

  echo "[OK] started $name  →  tmux attach -t $name"
}

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
start_tmux agg       "${AGG_CMD[@]}"
start_tmux data    "${DATA_SERVER_CMD[@]}"
start_tmux analysis  "${ANALYSIS_CMD[@]}"
start_tmux notify    "${NOTIFY_CMD[@]}"

echo "Please verify that the servers actually started (look in logs folder or attach to session). Common issue: Command not found (poetry/pyproject.toml error)" 