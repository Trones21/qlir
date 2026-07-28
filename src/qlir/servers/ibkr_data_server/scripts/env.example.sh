#!/usr/bin/env bash
# Copy to ~/set_ibkr_env_vars.sh, fill in, and `chmod 600` it.
# The startup scripts source this. Use a PAPER-trading account for unattended automation
# (paper logins have no 2FA prompt, so IBC can log in headlessly).

# --- Paper account credentials ---
export IB_USERNAME="your_paper_username"
export IB_PASSWORD="your_paper_password"

# --- Connection (defaults shown) ---
export IB_HOST="127.0.0.1"
export IB_PORT="4002"          # 4002 = paper gateway, 4001 = live gateway (7497/7496 = TWS)
export IB_TRADING_MODE="paper" # "paper" or "live"

# --- Install locations (only needed on the headless-server path) ---
export IBC_PATH="$HOME/ibc"
export TWS_PATH="$HOME/Jts"
export IBC_INI="$HOME/ibc/config.ini"
export TWS_MAJOR_VRSN="1030"   # IB Gateway major version installed (update to match)
export DISPLAY_NUM="99"        # Xvfb display used for the headless GUI
