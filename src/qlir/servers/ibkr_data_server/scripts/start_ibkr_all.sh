#!/usr/bin/env bash
#
# ONE COMMAND to bring up IBKR ingest. Detects the state, does what it can automatically,
# and always tells you exactly where things stand.
#
#   ./start_ibkr_all.sh [SYMBOL] [BAR_SIZE]
#   e.g. ./start_ibkr_all.sh AAPL 1m
#
# Behavior:
#   - If the Gateway is already reachable (you started the desktop app, OR it's already up)
#     -> just start the data server.
#   - Else if IBC is installed (headless-server path) -> auto-launch the Gateway, then start.
#   - Else -> print the exact manual steps (local dev path) and stop.
#
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

SYMBOL="${1:-AAPL}"
BAR_SIZE="${2:-1m}"

banner "QLIR IBKR bring-up  (symbol=$SYMBOL bar_size=$BAR_SIZE)"
load_ibkr_env

IB_HOST="${IB_HOST:-127.0.0.1}"
IB_PORT="${IB_PORT:-4002}"

# ---- 1) ensure the Gateway is reachable ----
if port_open "$IB_HOST" "$IB_PORT"; then
  ok "IB Gateway reachable at $IB_HOST:$IB_PORT"
else
  warn "IB Gateway NOT reachable at $IB_HOST:$IB_PORT"
  if [ -f "${IBC_PATH:-$HOME/ibc}/gatewaystart.sh" ]; then
    log "IBC detected -> launching Gateway headless (server path)"
    if ! "$SCRIPT_DIR/ib_gateway_up.sh"; then
      err "Automated Gateway launch failed — see the log above."
      exit 1
    fi
  else
    err "Gateway is not running and IBC is not installed. Choose one:"
    echo
    echo "  LOCAL DEV:  start the IB Gateway or TWS desktop app and LOG IN, then"
    echo "              enable the API (Settings -> API -> Enable ActiveX and Socket Clients),"
    echo "              confirm the port is $IB_PORT, and re-run this script."
    echo
    echo "  SERVER:     run  $SCRIPT_DIR/install_ib_gateway.sh  once, then create"
    echo "              ~/set_ibkr_env_vars.sh from scripts/env.example.sh (paper creds),"
    echo "              and re-run this script."
    echo
    exit 1
  fi
fi

# ---- 2) preflight ib_async (dependency) ----
if ! poetry run python -c "import ib_async" >/dev/null 2>&1; then
  err "ib_async not importable -> run: poetry install"
  exit 1
fi
ok "ib_async importable"

# ---- 3) start the data server in tmux ----
mkdir -p logs
SESSION="qlir_ibkr_data"
log "Starting ibkr_data_server in tmux session '$SESSION'"
tmux new-session -d -s "$SESSION" \
  "poetry run ibkr_data_server --symbol $SYMBOL --bar-size $BAR_SIZE --host $IB_HOST --port $IB_PORT 2>&1 | tee -a logs/ibkr_data_server.log"

# ---- 4) status summary ----
banner "IBKR ingest — STATE"
ok "gateway        : $IB_HOST:$IB_PORT (reachable)"
ok "symbol/bar     : $SYMBOL / $BAR_SIZE"
ok "tmux session   : $SESSION   (attach: tmux attach -t $SESSION)"
ok "server log     : logs/ibkr_data_server.log   (tail -f logs/ibkr_data_server.log)"
log "raw data will appear under: \$QLIR_DATA_ROOT/interactive_brokers/historical_bars/raw/$SYMBOL/$BAR_SIZE/"
tmux list-sessions 2>/dev/null || true
