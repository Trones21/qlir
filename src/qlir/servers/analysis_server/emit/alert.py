# analysis_server/emit/alert.py

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict


from qlir.servers.alerts.paths import get_alerts_root

ALERTS_DIR = get_alerts_root()
OUTBOX_REGISTRY_PATH = ALERTS_DIR / "analysis_outboxes.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -------------------------------
# Outbox registry
# -------------------------------

def write_outbox_registry(outboxes: Dict[str, Dict[str, Any]]) -> None:
    """
    Declare available outboxes.

    This is an authoritative, durable declaration.
    Notification servers discover outboxes from this file.
    """
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "version": 1,
        "generated_at": utc_now_iso(),
        "outboxes": outboxes,
    }

    OUTBOX_REGISTRY_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True)
    )


def ensure_outbox_declared(outbox: str) -> None:
    """
    Defensive check: ensure the outbox exists on disk.
    Registry validation is intentionally light here.
    """
    outbox_dir = ALERTS_DIR / outbox
    outbox_dir.mkdir(parents=True, exist_ok=True)


# -------------------------------
# Alert emission
# -------------------------------

def emit_alert(*, outbox: str, data: Any) -> None:
    """
    Emit an alert into a specific outbox.

    Contract:
    {
      "ts": <UTC now>,
      "outbox": <outbox name>,
      "data": <opaque payload>
    }
    """
    ensure_outbox_declared(outbox)

    alert = {
        "ts": utc_now_iso(),
        "outbox": outbox,
        "data": data,
    }

    # Filename is for uniqueness + debugging only
    fname = f"{alert['ts']}.json"
    path = ALERTS_DIR / outbox / fname

    path.write_text(json.dumps(alert))
