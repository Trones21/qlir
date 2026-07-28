#!/usr/bin/env bash
#
# Launch IB Gateway HEADLESS via IBC under a virtual display (server path).
# Preflights everything first and prints exactly what's missing before doing anything.
#
# ⚠️ UNTESTED here (needs a real box + a paper IBKR account). PAPER accounts only for
# unattended login — live accounts prompt for 2FA which cannot be auto-approved.
#
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
load_ibkr_env

IB_HOST="${IB_HOST:-127.0.0.1}"
IB_PORT="${IB_PORT:-4002}"
IBC_PATH="${IBC_PATH:-$HOME/ibc}"
TWS_PATH="${TWS_PATH:-$HOME/Jts}"
IBC_INI="${IBC_INI:-$IBC_PATH/config.ini}"
TWS_MAJOR_VRSN="${TWS_MAJOR_VRSN:-1030}"
DISPLAY_NUM="${DISPLAY_NUM:-99}"

banner "IB Gateway headless launch (IBC + xvfb)"

# ---- preflight: fail loud + actionable ----
fail=0
require_cmd Xvfb "sudo apt-get install -y xvfb  (or run scripts/install_ib_gateway.sh)" || fail=1
[ -f "$IBC_PATH/gatewaystart.sh" ] && ok "IBC found: $IBC_PATH" || { err "IBC missing at $IBC_PATH -> run scripts/install_ib_gateway.sh"; fail=1; }
[ -d "$TWS_PATH" ] && ok "IB Gateway dir: $TWS_PATH" || { err "IB Gateway missing at $TWS_PATH -> run scripts/install_ib_gateway.sh"; fail=1; }
[ -n "${IB_USERNAME:-}" ] && ok "IB_USERNAME set" || { err "IB_USERNAME not set -> see scripts/env.example.sh"; fail=1; }
[ -n "${IB_PASSWORD:-}" ] && ok "IB_PASSWORD set" || { err "IB_PASSWORD not set -> see scripts/env.example.sh"; fail=1; }
command -v envsubst >/dev/null 2>&1 || { err "envsubst missing -> sudo apt-get install -y gettext-base"; fail=1; }
if [ "$fail" -ne 0 ]; then err "Preflight failed — fix the items above and re-run."; exit 1; fi

if port_open "$IB_HOST" "$IB_PORT"; then
  ok "Gateway already reachable at $IB_HOST:$IB_PORT — nothing to do."
  exit 0
fi

# ---- render IBC config from template (creds come from env; file is chmod 600) ----
log "Rendering IBC config -> $IBC_INI"
IB_TRADING_MODE="${IB_TRADING_MODE:-paper}" \
  envsubst < "$SCRIPT_DIR/ibc_config.ini.template" > "$IBC_INI"
chmod 600 "$IBC_INI"

# ---- virtual display ----
if ! xdpyinfo -display ":$DISPLAY_NUM" >/dev/null 2>&1; then
  log "Starting Xvfb on :$DISPLAY_NUM"
  Xvfb ":$DISPLAY_NUM" -screen 0 1024x768x16 >/tmp/xvfb.log 2>&1 &
  sleep 2
fi
export DISPLAY=":$DISPLAY_NUM"

# ---- launch IBC (auto-login the paper Gateway) ----
log "Launching IB Gateway via IBC (mode=${IB_TRADING_MODE:-paper}, port=$IB_PORT)"
mkdir -p "$SCRIPT_DIR/../logs" 2>/dev/null || true
LOGDIR="${QLIR_IBKR_LOGDIR:-$HOME/qlir_ibkr_logs}"; mkdir -p "$LOGDIR"

TWS_MAJOR_VRSN="$TWS_MAJOR_VRSN" \
IBC_INI="$IBC_INI" \
TWS_PATH="$TWS_PATH" \
IBC_PATH="$IBC_PATH" \
  nohup "$IBC_PATH/gatewaystart.sh" >"$LOGDIR/ibc_gateway.log" 2>&1 &

# ---- wait for the API port ----
if wait_for_port "$IB_HOST" "$IB_PORT" 180; then
  banner "IB Gateway UP"
  ok "API listening at $IB_HOST:$IB_PORT (mode=${IB_TRADING_MODE:-paper})"
  log "IBC log: $LOGDIR/ibc_gateway.log"
  log "Xvfb log: /tmp/xvfb.log  (DISPLAY=:$DISPLAY_NUM)"
else
  err "Gateway did not come up. Check the IBC log: $LOGDIR/ibc_gateway.log"
  err "Common causes: wrong TWS_MAJOR_VRSN, bad paper credentials, or a 2FA-enabled (non-paper) account."
  exit 1
fi
