from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .alerts import AlertBackoffState, AlertKey, INITIAL_BACKOFF


ALERT_STATE_PATH = Path("alert_backoff_state.json")


def load_alert_states() -> dict[AlertKey, AlertBackoffState]:
    if not ALERT_STATE_PATH.exists():
        return {}

    raw = json.loads(ALERT_STATE_PATH.read_text(encoding="utf-8"))
    states: dict[AlertKey, AlertBackoffState] = {}

    for key, payload in raw.items():
        states[key] = AlertBackoffState(
            key=key,
            last_emitted_at=(
                datetime.fromisoformat(payload["last_emitted_at"])
                if payload.get("last_emitted_at") is not None
                else None
            ),
            backoff_sec=int(payload["backoff_sec"]),
        )

    return states


def save_alert_states(states: dict[AlertKey, AlertBackoffState]) -> None:
    payload = {
        key: {
            "last_emitted_at": (
                state.last_emitted_at.isoformat()
                if state.last_emitted_at is not None
                else None
            ),
            "backoff_sec": state.backoff_sec,
        }
        for key, state in states.items()
    }

    ALERT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ALERT_STATE_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
