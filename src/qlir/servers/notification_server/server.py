import json
import os
from pathlib import Path
import shutil
import time
from typing import Any

from .adapters.base import NotificationAdapter
from .adapters.telegram import TelegramAdapter
from .logging import setup_logging

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


OUTBOX_DIR = Path("alerts/outbox")
SENT_DIR = Path("alerts/sent")
FAILED_DIR = Path("alerts/failed")

POLL_INTERVAL_SEC = 2.0
MAX_RETRIES = 3  # minimal retry metadata


def load_alert(path: Path) -> dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def increment_retry(alert: dict[str, Any]) -> None:
    meta = alert.setdefault("_meta", {})
    meta["retries"] = meta.get("retries", 0) + 1


def retries_exceeded(alert: dict[str, Any]) -> bool:
    return alert.get("_meta", {}).get("retries", 0) >= MAX_RETRIES


def main() -> None:
    logger = setup_logging()

    for d in (OUTBOX_DIR, SENT_DIR, FAILED_DIR):
        d.mkdir(parents=True, exist_ok=True)

    adapters: list[NotificationAdapter] = [
        # WebhookAdapter("https://example.com/webhook"),
        TelegramAdapter(
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
    ),
        # add more adapters here later
    ]

    logger.info("notification server started")

    while True:
        for alert_path in sorted(OUTBOX_DIR.glob("*.json")):
            try:
                alert = load_alert(alert_path)

                if "ts" not in alert or "data" not in alert:
                    raise ValueError("invalid alert contract")

                for adapter in adapters:
                    adapter.send(alert["data"])

                shutil.move(alert_path, SENT_DIR / alert_path.name)
                logger.info("sent alert %s", alert_path.name)

            except Exception as e:
                logger.warning("failed alert %s: %s", alert_path.name, e)

                try:
                    alert = load_alert(alert_path)
                    increment_retry(alert)

                    if retries_exceeded(alert):
                        shutil.move(alert_path, FAILED_DIR / alert_path.name)
                        logger.error("alert %s moved to failed", alert_path.name)
                    else:
                        with alert_path.open("w") as f:
                            json.dump(alert, f)

                except Exception as inner:
                    logger.error("failed to update retry metadata: %s", inner)

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
