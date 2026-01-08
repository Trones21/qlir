from typing import Any
import json
import requests

from .base import NotificationAdapter


class TelegramAdapter(NotificationAdapter):
    def __init__(self, bot_token: str, chat_id: str, timeout: float = 5.0):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout

        self.url = (
            f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        )

    def send(self, data: Any) -> None:
        """
        Send alert payload to Telegram.

        Data is treated as opaque:
        - If it's a string, send directly
        - Otherwise, JSON-dump it
        """
        if isinstance(data, str):
            text = data
        else:
            text = json.dumps(data, indent=2, sort_keys=True)

        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        resp = requests.post(
            self.url,
            json=payload,
            timeout=self.timeout,
        )
        if not resp.ok:
            raise RuntimeError(f"{resp.status_code}: {resp.text}")
        resp.raise_for_status()
