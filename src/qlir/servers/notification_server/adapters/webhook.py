from typing import Any
import requests

from .base import NotificationAdapter


class WebhookAdapter(NotificationAdapter):
    def __init__(self, url: str, timeout: float = 5.0):
        self.url = url
        self.timeout = timeout

    def send(self, data: Any) -> None:
        resp = requests.post(
            self.url,
            json=data,
            timeout=self.timeout,
        )
        resp.raise_for_status()
