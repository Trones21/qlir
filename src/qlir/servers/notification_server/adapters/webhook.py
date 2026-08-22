from typing import Any, Mapping

import requests

from .base import NotificationAdapter


class WebhookAdapter(NotificationAdapter):
    """
    POST the alert payload as JSON to an arbitrary HTTP endpoint.

    This is the general-purpose integration sink: the alert body is already a
    JSON document, so any system that can accept a webhook -- an internal
    service, a queue gateway, Slack/Discord, an AWS API Gateway fronting SNS or
    Lambda -- can consume it without qlir needing to know anything about it.
    """

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: Mapping[str, str] | None = None,
        timeout: float = 10.0,
    ):
        self.url = url
        self.method = method.upper()
        self.headers = dict(headers or {})
        self.timeout = timeout

    def send(self, data: Any) -> None:
        resp = requests.request(
            self.method,
            self.url,
            json=data,
            headers=self.headers or None,
            timeout=self.timeout,
        )
        if not resp.ok:
            raise RuntimeError(f"{resp.status_code}: {resp.text[:500]}")
