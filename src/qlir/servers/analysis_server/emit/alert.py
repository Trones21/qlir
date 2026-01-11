# analysis_server/emit/alert.py

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

ALERT_OUTBOX = Path("alerts/outbox")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_alert(data: Any) -> None:
    """
    Emit an alert event.

    Contract:
    {
      "ts": <UTC now>,
      "data": <opaque payload>
    }
    """
    ALERT_OUTBOX.mkdir(parents=True, exist_ok=True)

    alert = {
        "ts": utc_now_iso(),
        "data": data,
    }

    # filename is for uniqueness + debugging only
    fname = f"{alert['ts']}.json"
    path = ALERT_OUTBOX / fname

    path.write_text(json.dumps(alert))
