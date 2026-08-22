"""
The catalogue of available notification sink types.

Each entry declares, in one place:
  - what config keys the sink type accepts, and which are required
  - which of those keys name an environment variable holding a secret
  - how to build the adapter
  - human setup guidance, surfaced verbatim in startup errors and by
    `poetry run notify_smoke`

Adding a transport means adding one SinkType here plus its adapter class.
Nothing else in the server needs to know the type exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any, Callable, Mapping

from .base import NotificationAdapter
from .console import ConsoleAdapter
from .email import EmailAdapter
from .file import FileAdapter
from .telegram import TelegramAdapter
from .webhook import WebhookAdapter


class SinkConfigError(RuntimeError):
    """Raised when a sink's configuration is missing, malformed, or unusable."""


@dataclass(frozen=True)
class SinkType:
    name: str
    summary: str
    build: Callable[[Mapping[str, Any]], NotificationAdapter]
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    # config keys whose VALUE is the NAME of an env var that must be set
    env_keys: tuple[str, ...] = ()
    setup_steps: tuple[str, ...] = field(default=())
    needs_network: bool = False


def _require(cfg: Mapping[str, Any], key: str, sink: str) -> Any:
    if key not in cfg:
        raise SinkConfigError(f"sink {sink!r}: missing required key {key!r}")
    return cfg[key]


def _env(cfg: Mapping[str, Any], key: str, sink: str) -> str:
    var = _require(cfg, key, sink)
    try:
        return os.environ[var]
    except KeyError:
        raise SinkConfigError(
            f"sink {sink!r}: config key {key!r} names environment variable {var!r}, "
            f"which is not set"
        ) from None


def _build_console(cfg: Mapping[str, Any]) -> NotificationAdapter:
    return ConsoleAdapter(stream=cfg.get("stream", "stdout"), pretty=cfg.get("pretty", True))


def _build_file(cfg: Mapping[str, Any]) -> NotificationAdapter:
    return FileAdapter(path=_require(cfg, "path", cfg.get("_name", "file")))


def _build_telegram(cfg: Mapping[str, Any]) -> NotificationAdapter:
    name = cfg.get("_name", "telegram")
    return TelegramAdapter(
        bot_token=_env(cfg, "bot_token_env", name),
        chat_id=_env(cfg, "chat_id_env", name),
    )


def _build_webhook(cfg: Mapping[str, Any]) -> NotificationAdapter:
    name = cfg.get("_name", "webhook")
    if "url_env" in cfg:
        url = _env(cfg, "url_env", name)
    elif "url" in cfg:
        url = cfg["url"]
    else:
        raise SinkConfigError(f"sink {name!r}: webhook requires either 'url' or 'url_env'")

    headers = dict(cfg.get("headers", {}))
    for header, var in (cfg.get("headers_from_env") or {}).items():
        try:
            headers[header] = os.environ[var]
        except KeyError:
            raise SinkConfigError(
                f"sink {name!r}: headers_from_env maps header {header!r} to environment "
                f"variable {var!r}, which is not set"
            ) from None

    return WebhookAdapter(
        url,
        method=cfg.get("method", "POST"),
        headers=headers,
        timeout=cfg.get("timeout", 10.0),
    )


def _build_email(cfg: Mapping[str, Any]) -> NotificationAdapter:
    name = cfg.get("_name", "email")
    to_addrs = _require(cfg, "to_addrs", name)
    if isinstance(to_addrs, str):
        to_addrs = [to_addrs]

    username = _env(cfg, "username_env", name) if "username_env" in cfg else None
    password = _env(cfg, "password_env", name) if "password_env" in cfg else None

    return EmailAdapter(
        smtp_host=_require(cfg, "smtp_host", name),
        smtp_port=int(cfg.get("smtp_port", 587)),
        from_addr=_require(cfg, "from_addr", name),
        to_addrs=to_addrs,
        username=username,
        password=password,
        use_tls=cfg.get("use_tls", True),
        use_ssl=cfg.get("use_ssl", False),
        subject_prefix=cfg.get("subject_prefix", "[qlir]"),
        timeout=cfg.get("timeout", 15.0),
    )


SINK_TYPES: dict[str, SinkType] = {
    "console": SinkType(
        name="console",
        summary="Print the alert to stdout/stderr. No setup, no network, no credentials.",
        build=_build_console,
        optional=("stream", "pretty"),
        setup_steps=("Nothing to set up. This sink always works.",),
    ),
    "file": SinkType(
        name="file",
        summary="Append the alert to a local JSONL file.",
        build=_build_file,
        required=("path",),
        setup_steps=(
            "Choose a writable path, e.g. path = \"~/qlir_alerts.jsonl\".",
            "Parent directories are created automatically.",
            "Tail it with: tail -f ~/qlir_alerts.jsonl",
        ),
    ),
    "telegram": SinkType(
        name="telegram",
        summary="Send the alert as a Telegram message via a bot.",
        build=_build_telegram,
        required=("bot_token_env", "chat_id_env"),
        env_keys=("bot_token_env", "chat_id_env"),
        needs_network=True,
        setup_steps=(
            "Open Telegram and message @BotFather; send /newbot and follow the prompts.",
            "BotFather replies with a bot token -- export it as the env var named by "
            "'bot_token_env'.",
            "Get your numeric chat id by messaging @userinfobot, then export it as the env "
            "var named by 'chat_id_env'. It is your user id, so it is the same for every bot "
            "you own.",
            "Send your new bot any message first -- a bot cannot open a conversation with you.",
            "Verify with: poetry run notify_smoke --sink <sink-name>",
        ),
    ),
    "webhook": SinkType(
        name="webhook",
        summary="POST the alert JSON to any HTTP endpoint (your service, Slack, SNS, Lambda...).",
        build=_build_webhook,
        required=(),  # exactly one of url / url_env, checked in the builder
        optional=("url", "url_env", "method", "headers", "headers_from_env", "timeout"),
        env_keys=("url_env",),
        needs_network=True,
        setup_steps=(
            "Set either 'url' (non-secret) or 'url_env' (secret URL held in the environment).",
            "The alert's 'data' object is sent as the JSON request body.",
            "Any non-2xx response is treated as a failure and retried.",
            "Confirm the host is reachable from wherever the notification server runs.",
            "Verify with: poetry run notify_smoke --sink <sink-name>",
        ),
    ),
    "email": SinkType(
        name="email",
        summary="Deliver the alert as an email over SMTP.",
        build=_build_email,
        required=("smtp_host", "from_addr", "to_addrs"),
        optional=(
            "smtp_port", "username_env", "password_env", "use_tls", "use_ssl",
            "subject_prefix", "timeout",
        ),
        env_keys=("username_env", "password_env"),
        needs_network=True,
        setup_steps=(
            "Point smtp_host/smtp_port at your provider (Gmail: smtp.gmail.com:587, "
            "AWS SES: email-smtp.<region>.amazonaws.com:587).",
            "Put the username and password in environment variables and name those vars "
            "in 'username_env' / 'password_env'. Never put the secret in the config file.",
            "Gmail requires an App Password, not your account password -- regular passwords "
            "are rejected.",
            "AWS SES requires SMTP credentials generated in the SES console; your IAM keys "
            "will not work, and a sandboxed SES account can only send to verified addresses.",
            "Outbound port 587 is blocked by many cloud hosts by default -- check before "
            "assuming the credentials are wrong.",
            "Verify with: poetry run notify_smoke --sink <sink-name>",
        ),
    ),
}


def get_sink_type(type_name: str) -> SinkType:
    try:
        return SINK_TYPES[type_name]
    except KeyError:
        raise SinkConfigError(
            f"unknown sink type {type_name!r}; available types: {', '.join(sorted(SINK_TYPES))}"
        ) from None
