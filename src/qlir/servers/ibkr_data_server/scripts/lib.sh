#!/usr/bin/env bash
# Shared helpers for the IBKR startup scripts.
# Philosophy: always print WHAT is needed, WHAT is missing, and the exact fix — so you
# rarely have to open the docs.

# Colors (fall back to plain if not a tty)
if [ -t 1 ]; then
  _C_BLUE="\033[1;34m"; _C_GREEN="\033[1;32m"; _C_YEL="\033[1;33m"; _C_RED="\033[1;31m"; _C_OFF="\033[0m"
else
  _C_BLUE=""; _C_GREEN=""; _C_YEL=""; _C_RED=""; _C_OFF=""
fi

banner() { echo; echo "============================================================"; echo " $*"; echo "============================================================"; }
log()  { echo -e "${_C_BLUE}[INFO]${_C_OFF} $*"; }
ok()   { echo -e "${_C_GREEN}[ OK ]${_C_OFF} $*"; }
warn() { echo -e "${_C_YEL}[WARN]${_C_OFF} $*" >&2; }
err()  { echo -e "${_C_RED}[FAIL]${_C_OFF} $*" >&2; }

# require_cmd <cmd> <fix-hint>
require_cmd() {
  if command -v "$1" >/dev/null 2>&1; then
    ok "found: $1"
    return 0
  fi
  err "missing: $1  ->  $2"
  return 1
}

# port_open <host> <port>  (uses bash /dev/tcp; no extra tooling needed)
port_open() {
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 2>/dev/null; return 0; }
  return 1
}

# wait_for_port <host> <port> <timeout_sec>
wait_for_port() {
  local host="$1" port="$2" timeout="${3:-120}" waited=0
  log "Waiting for $host:$port (timeout ${timeout}s)..."
  while ! port_open "$host" "$port"; do
    sleep 3; waited=$((waited + 3))
    if [ "$waited" -ge "$timeout" ]; then
      err "$host:$port did not open within ${timeout}s"
      return 1
    fi
    log "  ...still waiting (${waited}s)"
  done
  ok "$host:$port is open"
  return 0
}

# load creds if present (paper account: IB_USERNAME / IB_PASSWORD, plus optional IB_HOST/IB_PORT)
load_ibkr_env() {
  local f="${IBKR_ENV_FILE:-$HOME/set_ibkr_env_vars.sh}"
  if [ -f "$f" ]; then
    # shellcheck disable=SC1090
    source "$f"
    ok "loaded IBKR env from $f"
  else
    warn "no IBKR env file at $f (see scripts/env.example.sh)"
  fi
}
