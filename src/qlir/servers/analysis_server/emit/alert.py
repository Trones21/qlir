# analysis_server/emit/alert.py

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict


from qlir.servers.alerts.paths import get_alerts_root

# NOTE: the alerts root is resolved lazily, never at import time. `get_alerts_root()`
# raises when QLIR_ALERTS_DIR is unset, and resolving it at module scope made merely
# *importing* server.py (e.g. to collect a test, or to read `_collect_required_df_names`)
# fail on any machine that had not exported it yet.


def alerts_dir() -> Path:
    return get_alerts_root()


def outbox_registry_path() -> Path:
    return alerts_dir() / "analysis_outboxes.json"


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
    root = alerts_dir()
    root.mkdir(parents=True, exist_ok=True)

    payload = {
        "version": 1,
        "generated_at": utc_now_iso(),
        "outboxes": outboxes,
    }

    outbox_registry_path().write_text(
        json.dumps(payload, indent=2, sort_keys=True)
    )


def ensure_outbox_declared(outbox: str) -> None:
    """
    Defensive check: ensure the outbox exists on disk.
    Registry validation is intentionally light here.
    """
    outbox_dir = alerts_dir() / outbox
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
    path = alerts_dir() / outbox / fname

    path.write_text(json.dumps(alert))
