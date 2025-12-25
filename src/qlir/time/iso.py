from __future__ import annotations

from datetime import datetime, timezone


def now_utc() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


def now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return now_utc().isoformat()

def parse_iso(value: str) -> datetime:
    """
    Parse ISO-8601 string into timezone-aware datetime.
    Accepts strings produced by datetime.isoformat().
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        # Defensive: assume UTC if tz missing
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
