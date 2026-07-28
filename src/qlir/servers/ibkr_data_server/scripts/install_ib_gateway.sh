#!/usr/bin/env bash
#
# Install the headless-server prerequisites for automated IB Gateway startup:
#   - xvfb (virtual display; IB Gateway is a Java GUI app)
#   - IB Gateway (standalone; bundles its own JRE)
#   - IBC (IbcAlpha) — auto-launches + auto-logs-in the Gateway
#
# ⚠️ UNTESTED in this environment (needs a real Linux box with internet). It is written to
# be idempotent and to log exactly what it does / what to fix. Only needed for the
# HEADLESS-SERVER path. On a local dev machine you just install the IB Gateway/TWS desktop
# app yourself and log in.
#
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
load_ibkr_env

# Pinned versions — bump as needed.
IBC_VERSION="${IBC_VERSION:-3.20.0}"
IBC_PATH="${IBC_PATH:-$HOME/ibc}"
TWS_PATH="${TWS_PATH:-$HOME/Jts}"
IBGW_INSTALLER_URL="${IBGW_INSTALLER_URL:-https://download2.interactivebrokers.com/installers/ibgateway/stable-standalone/ibgateway-stable-standalone-linux-x64.sh}"
IBC_ZIP_URL="${IBC_ZIP_URL:-https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBCLinux-${IBC_VERSION}.zip}"

banner "IBKR headless install (xvfb + IB Gateway + IBC)"

# 1) apt prerequisites
if command -v apt-get >/dev/null 2>&1; then
  log "Installing apt prerequisites (xvfb, curl, unzip)..."
  sudo apt-get update -y
  sudo apt-get install -y xvfb curl unzip
else
  warn "apt-get not found — install 'xvfb', 'curl', 'unzip' with your package manager."
fi

# 2) IB Gateway (standalone; bundles a JRE, so no separate Java needed)
if [ -d "$TWS_PATH/ibgateway" ]; then
  ok "IB Gateway already present under $TWS_PATH/ibgateway"
else
  log "Downloading IB Gateway standalone installer..."
  if curl -fLo /tmp/ibgw-install.sh "$IBGW_INSTALLER_URL"; then
    chmod +x /tmp/ibgw-install.sh
    log "Running silent install to $TWS_PATH (install4j)..."
    # install4j silent flags; if this fails, run interactively: /tmp/ibgw-install.sh
    if ! /tmp/ibgw-install.sh -q -dir "$TWS_PATH"; then
      warn "Silent install returned non-zero. Run it interactively once: /tmp/ibgw-install.sh"
    fi
  else
    err "Failed to download IB Gateway from $IBGW_INSTALLER_URL"
    err "  -> download it manually from interactivebrokers.com and install to $TWS_PATH"
  fi
fi

# 3) IBC
if [ -f "$IBC_PATH/gatewaystart.sh" ]; then
  ok "IBC already present at $IBC_PATH"
else
  log "Downloading IBC $IBC_VERSION..."
  if curl -fLo /tmp/ibc.zip "$IBC_ZIP_URL"; then
    mkdir -p "$IBC_PATH"
    unzip -o /tmp/ibc.zip -d "$IBC_PATH" >/dev/null
    chmod +x "$IBC_PATH"/*.sh 2>/dev/null || true
    ok "IBC installed to $IBC_PATH"
  else
    err "Failed to download IBC from $IBC_ZIP_URL"
    err "  -> check the latest version at https://github.com/IbcAlpha/IBC/releases and set IBC_VERSION"
  fi
fi

banner "Install summary"
[ -d "$TWS_PATH/ibgateway" ] && ok "IB Gateway: $TWS_PATH/ibgateway" || err "IB Gateway: NOT installed"
[ -f "$IBC_PATH/gatewaystart.sh" ] && ok "IBC: $IBC_PATH" || err "IBC: NOT installed"
command -v Xvfb >/dev/null 2>&1 && ok "xvfb: installed" || err "xvfb: NOT installed"
echo
log "Next: create ~/set_ibkr_env_vars.sh from scripts/env.example.sh (paper creds),"
log "      then run: scripts/start_ibkr_all.sh <SYMBOL> <BAR_SIZE>"
