#!/bin/bash/

# This script installs the dependcencies and sets up the servers  

#!/usr/bin/env bash

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
  warn "pip not found. Attempting to bootstrap pip..."
  $PYTHON -m ensurepip --upgrade || error "Failed to install pip"
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

