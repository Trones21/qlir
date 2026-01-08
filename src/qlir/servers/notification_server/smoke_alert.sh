#!/usr/bin/env bash
set -euo pipefail

OUTBOX_DIR="alerts/outbox"

mkdir -p "$OUTBOX_DIR"

TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
EPOCH="$(date -u +"%s")"

ALERT_FILE="$OUTBOX_DIR/${EPOCH}_smoke_test.json"

cat > "$ALERT_FILE" <<EOF
{
  "ts": "$TS",
  "data": {
    "type": "smoke_test",
    "message": "hello world",
    "epoch": $EPOCH,
    "host": "$(hostname)"
  }
}
EOF

echo "wrote smoke alert: $ALERT_FILE"
