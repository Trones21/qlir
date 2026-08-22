import json
from pathlib import Path
from typing import Any

from .base import NotificationAdapter


class FileAdapter(NotificationAdapter):
    """
    Append the alert to a JSONL file, one JSON object per line.

    Useful as a durable local audit trail, and as a sink you can tail or feed
    into another process without standing up a transport.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()

    def send(self, data: Any) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(data, sort_keys=True, default=str)
        with self.path.open("a") as f:
            f.write(line + "\n")
