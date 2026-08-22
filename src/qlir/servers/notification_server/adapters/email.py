from email.message import EmailMessage
import json
import smtplib
from typing import Any, Sequence

from .base import NotificationAdapter


class EmailAdapter(NotificationAdapter):
    """
    Deliver the alert as an email over SMTP.

    Credentials come from environment variables, never from the config file --
    the config names the env var, the environment holds the secret, so
    notifications.toml stays safe to read and to share.
    """

    def __init__(
        self,
        *,
        smtp_host: str,
        smtp_port: int,
        from_addr: str,
        to_addrs: Sequence[str],
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
        use_ssl: bool = False,
        subject_prefix: str = "[qlir]",
        timeout: float = 15.0,
    ):
        if not to_addrs:
            raise ValueError("email sink: 'to_addrs' must list at least one recipient")

        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_addr = from_addr
        self.to_addrs = list(to_addrs)
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.subject_prefix = subject_prefix
        self.timeout = timeout

    def _subject(self, data: Any) -> str:
        if isinstance(data, dict):
            trigger = data.get("trigger") or data.get("type") or "alert"
            return f"{self.subject_prefix} {trigger}"
        return f"{self.subject_prefix} alert"

    def _body(self, data: Any) -> str:
        if isinstance(data, str):
            return data
        return json.dumps(data, indent=2, sort_keys=True, default=str)

    def send(self, data: Any) -> None:
        msg = EmailMessage()
        msg["Subject"] = self._subject(data)
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(self.to_addrs)
        msg.set_content(self._body(data))

        if self.use_ssl:
            server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=self.timeout)
        else:
            server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout)

        try:
            server.ehlo()
            if self.use_tls and not self.use_ssl:
                server.starttls()
                server.ehlo()
            if self.username is not None and self.password is not None:
                server.login(self.username, self.password)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                # the alert was already handed to the server; a noisy teardown
                # must not turn a delivered alert into a retry
                pass
