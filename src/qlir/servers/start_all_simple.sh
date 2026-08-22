#!/usr/bin/env bash
# Bring up the four core pipeline services, each in its own tmux session.
#
#   data_server -> agg_server -> analysis_server -> notification_server
#
# Services are decoupled and start in any order: analysis waits for agg to
# produce parquet, and notification idles until alerts appear. Starting them
# together is fine.
#
# Override before running if you want something other than the defaults:
#   SYMBOL=BTCUSDT INTERVAL=1s ./start_all_simple.sh

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# --- config ---------------------------------------------------------------
SYMBOL="${SYMBOL:-SOLUSDT}"
INTERVAL="${INTERVAL:-1m}"
LIMIT="${LIMIT:-1000}"
BATCH_SLICES="${BATCH_SLICES:-1000}"
LOG_DIR="${LOG_DIR:-$HOME/logs}"

# These must be EXPORTED, not just assigned: the tmux child processes read them
# from the environment, and a bare assignment is not inherited.
export QLIR_ALERTS_DIR="${QLIR_ALERTS_DIR:-$HOME/alerts}"
export QLIR_MANIFEST_LOG="${QLIR_MANIFEST_LOG:-1}"
export QLIR_ANALYSIS_SYMBOL="${QLIR_ANALYSIS_SYMBOL:-$SYMBOL}"
export QLIR_ANALYSIS_INTERVAL="${QLIR_ANALYSIS_INTERVAL:-$INTERVAL}"
export QLIR_ANALYSIS_LIMIT="${QLIR_ANALYSIS_LIMIT:-$LIMIT}"

# Telegram credentials are optional. Without them the notification server falls
# back to console delivery (or whatever notifications.toml says), so the pipeline
# still runs end to end. Sourcing this unconditionally used to abort the script
# on any machine that did not happen to have the file.
TELEGRAM_ENV_FILE="${TELEGRAM_ENV_FILE:-$HOME/set_telegram_env_vars.sh}"
if [ -f "$TELEGRAM_ENV_FILE" ]; then
    # shellcheck source=/dev/null
    source "$TELEGRAM_ENV_FILE"
    echo "sourced telegram env vars from $TELEGRAM_ENV_FILE"
else
    echo "note: $TELEGRAM_ENV_FILE not found -- skipping."
    echo "      Notification delivery follows notifications.toml, or falls back to"
    echo "      the console if there is no config. See notifications.example.toml."
fi

mkdir -p "$LOG_DIR" "$QLIR_ALERTS_DIR"

# --- preflight ------------------------------------------------------------
if ! command -v tmux >/dev/null 2>&1; then
    echo "error: tmux is not installed. Run ./install_system_deps.sh" >&2
    exit 1
fi

if ! (cd "$REPO_ROOT" && poetry run python -c "import qlir" >/dev/null 2>&1); then
    echo "error: the qlir package is not importable in the poetry env." >&2
    echo "       Run: cd $REPO_ROOT && poetry install --with dev" >&2
    exit 1
fi

echo "repo:      $REPO_ROOT"
echo "symbol:    $SYMBOL $INTERVAL (limit=$LIMIT)"
echo "alerts:    $QLIR_ALERTS_DIR"
echo "logs:      $LOG_DIR"
echo

# --- start ----------------------------------------------------------------
start() {
    local session="$1"; shift
    local logfile="$1"; shift
    local cmd="$*"

    if tmux has-session -t "$session" 2>/dev/null; then
        echo "  $session already running -- leaving it alone"
        return
    fi

    tmux new-session -d -s "$session" \
        "cd '$REPO_ROOT' && $cmd 2>&1 | tee -a '$LOG_DIR/$logfile'"
    echo "  started $session"
}

start qlir_data     data_server.log \
    "poetry run data_server --endpoint klines --symbol $SYMBOL --interval $INTERVAL --limit $LIMIT"
start qlir_agg      agg_server.log \
    "poetry run agg_server --endpoint klines --symbol $SYMBOL --interval $INTERVAL --limit $LIMIT --batch-slices $BATCH_SLICES"
start qlir_analysis analysis_server.log \
    "poetry run analysis_server"
start qlir_notify   notifications_server.log \
    "poetry run notifications_server"

echo
tmux list-sessions
cat <<EOF

Attach to a service:   tmux attach -t qlir_analysis
Stop everything:       tmux kill-session -t qlir_data \\
                       tmux kill-session -t qlir_agg \\
                       tmux kill-session -t qlir_analysis \\
                       tmux kill-session -t qlir_notify

The analysis server will sit and wait until the agg server has sealed its first
parquet chunk. That is expected; it logs what it is waiting on every 30s.
EOF
