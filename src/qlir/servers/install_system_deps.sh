#!/usr/bin/env bash

# This script installs the dependencies and sets up the servers

set -euo pipefail

############################################
# Utils
############################################

log() {
  echo -e "\033[1;34m[INFO]\033[0m $*"
}

warn() {
  echo -e "\033[1;33m[WARN]\033[0m $*" >&2
}

error() {
  echo -e "\033[1;31m[ERROR]\033[0m $*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

############################################
# tmux
############################################

log "Checking for tmux..."
if command_exists tmux; then
  log "tmux already installed: $(tmux -V)"
else
  warn "tmux not found. Installing..."

  if command_exists apt-get; then
    sudo apt-get update
    sudo apt-get install -y tmux
  elif command_exists dnf; then
    sudo dnf install -y tmux
  elif command_exists pacman; then
    sudo pacman -S --noconfirm tmux
  elif command_exists brew; then
    brew install tmux
  else
    error "No supported package manager found to install tmux"
  fi
fi

log "tmux ready: $(tmux -V)"

############################################
# Git
############################################

log "Checking for git..."
if ! command_exists git; then
  error "git not found. Please install git first."
fi
log "git found: $(git --version)"

############################################
# Python
############################################

log "Checking for Python 3..."
if command_exists python3; then
  PYTHON=python3
elif command_exists python; then
  PYTHON=python
else
  error "Python not found. Please install Python 3.8+."
fi

PYTHON_VERSION="$($PYTHON -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
log "Python found: $PYTHON ($PYTHON_VERSION)"

############################################
# pip
############################################

log "Checking for pip..."
if ! $PYTHON -m pip --version >/dev/null 2>&1; then
  warn "pip not found. Installing..."

  # 1) Try the stdlib bootstrap (works when ensurepip is present).
  if $PYTHON -m ensurepip --upgrade >/dev/null 2>&1; then
    log "pip bootstrapped via ensurepip"
  # 2) Fall back to the system package manager (ensurepip is often absent on Debian/Ubuntu).
  elif command_exists apt-get; then
    sudo apt-get update
    sudo apt-get install -y python3-pip
  elif command_exists dnf; then
    sudo dnf install -y python3-pip
  elif command_exists pacman; then
    sudo pacman -S --noconfirm python-pip
  elif command_exists brew; then
    warn "On macOS pip ships with python3; ensure Python was installed via brew/python.org"
  # 3) Last resort: official get-pip.py bootstrap script.
  elif command_exists curl; then
    warn "Falling back to get-pip.py..."
    curl -sSL https://bootstrap.pypa.io/get-pip.py | $PYTHON - || error "Failed to install pip"
  else
    error "Could not install pip: no ensurepip, package manager, or curl available"
  fi

  $PYTHON -m pip --version >/dev/null 2>&1 || error "pip installation did not succeed"
fi

log "pip found: $($PYTHON -m pip --version)"

############################################
# Poetry
############################################

log "Checking for Poetry..."
if command_exists poetry; then
  log "Poetry already installed: $(poetry --version)"
else
  log "Installing Poetry..."
  curl -sSL https://install.python-poetry.org | $PYTHON - || error "Poetry installation failed"
fi

############################################
# PATH check
############################################

POETRY_BIN="$HOME/.local/bin"

if [[ ":$PATH:" != *":$POETRY_BIN:"* ]]; then
  warn "Poetry bin directory not in PATH"
  warn "Add this to your shell config:"
  echo
  echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
  echo
fi

log "Poetry version: $($HOME/.local/bin/poetry --version 2>/dev/null || poetry --version)"

echo "System deps installed"

