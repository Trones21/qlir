#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------

# Alerts root (must match alerts.paths)
ALERTS_ROOT="${QLIR_ALERTS_DIR:-alerts}"

# Which outbox to test (default: ops)
OUTBOX="${1:-ops}"

OUTBOX_DIR="${ALERTS_ROOT}/${OUTBOX}"

mkdir -p "$OUTBOX_DIR"

# -----------------------------
# Alert payload
# -----------------------------

TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
EPOCH="$(date -u +"%s")"
HOST="$(hostname)"

ALERT_FILE="${OUTBOX_DIR}/${EPOCH}_smoke_test.json"

cat > "$ALERT_FILE" <<EOF
{
  "ts": "$TS",
  "outbox": "$OUTBOX",
  "data": {
    "type": "smoke_test",
    "message": "hello world",
    "epoch": $EPOCH,
    "host": "$HOST"
  }
}
EOF

echo "wrote smoke alert:"
echo "  outbox: $OUTBOX"
echo "  file:   $ALERT_FILE"
