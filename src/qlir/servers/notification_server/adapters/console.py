import json
import sys
from typing import Any

from .base import NotificationAdapter


class ConsoleAdapter(NotificationAdapter):
    """
    Write the alert to stdout/stderr.

    This is the zero-configuration sink. It requires no credentials, no network,
    and no external account, which makes it the default when no notifications
    config exists -- so the notification server is runnable on a fresh clone and
    the pipeline can be exercised end to end before any transport is set up.
    """

    def __init__(self, stream: str = "stdout", pretty: bool = True):
        if stream not in ("stdout", "stderr"):
            raise ValueError(f"console sink: stream must be 'stdout' or 'stderr', got {stream!r}")
        self.stream_name = stream
        self.pretty = pretty

    def _stream(self):
        return sys.stdout if self.stream_name == "stdout" else sys.stderr

    def send(self, data: Any) -> None:
        if isinstance(data, str):
            text = data
        elif self.pretty:
            text = json.dumps(data, indent=2, sort_keys=True, default=str)
        else:
            text = json.dumps(data, sort_keys=True, default=str)

        stream = self._stream()
        stream.write(f"[qlir alert] {text}\n")
        stream.flush()
