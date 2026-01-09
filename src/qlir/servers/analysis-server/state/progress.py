# analysis_server/state/progress.py

import json
from pathlib import Path
from typing import Optional
import pandas as pd

STATE_FILE = Path("analysis_state.json")


def load_last_processed_ts() -> Optional[pd.Timestamp]:
    """
    Load the last processed data timestamp (UTC).
    Returns None if no state exists.
    """
    if not STATE_FILE.exists():
        return None

    raw = json.loads(STATE_FILE.read_text())
    return pd.Timestamp(raw["last_processed_ts"], tz="UTC")


def save_last_processed_ts(ts: pd.Timestamp) -> None:
    """
    Persist the last processed data timestamp.
    """
    STATE_FILE.write_text(
        json.dumps({"last_processed_ts": ts.isoformat()})
    )
