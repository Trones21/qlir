#!/usr/bin/env bash
set -euo pipefail

# positional args
SESSION_NAME="${1:?session name required}"
LOG_DIR="${2:?log dir required}"
shift 2

if [ "$#" -eq 0 ]; then
  echo "[ERROR] command required" >&2
  exit 2
fi

LOG_FILE="$LOG_DIR/$SESSION_NAME.log"

mkdir -p "$LOG_DIR"

echo "[INFO] starting $SESSION_NAME" | tee -a "$LOG_FILE" >&2
echo "[INFO] cmd: $*" | tee -a "$LOG_FILE" >&2

"$@" 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}

if [ "$status" -ne 0 ]; then
  echo "[ERROR] $SESSION_NAME exited with status $status" | tee -a "$LOG_FILE"
fi

echo "[INFO] $SESSION_NAME finished (status=$status)" | tee -a "$LOG_FILE"
