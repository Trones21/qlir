import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Iterable

from .adapters.base import NotificationAdapter
from .adapters.telegram import TelegramAdapter
from .logging import setup_logging

from qlir.servers.alerts.paths import get_alerts_root


ALERTS_ROOT = get_alerts_root()
SENT_ROOT = ALERTS_ROOT / "_sent"
FAILED_ROOT = ALERTS_ROOT / "_failed"

POLL_INTERVAL_SEC = 2.0
MAX_RETRIES = 3


# -------------------------------------------------
# Outbox → adapter routing (AUTHORITATIVE)
# -------------------------------------------------

OUTBOX_ROUTES: dict[str, list[dict[str, str]]] = {
    
    "qlir-ops": [
        {
            "adapter": "telegram",
            "bot_token_env": "OPS_TELEGRAM_BOT_TOKEN",
            "chat_id_env": "TELEGRAM_CHAT_ID", # Note: Telegram uses your user id for the chat id, so it'll be the same for all your bots"
        }
    ],
    "qlir-data-pipeline": [
        {
            "adapter": "telegram",
            "bot_token_env": "DATA_PIPELINE_TELEGRAM_BOT_TOKEN",
            "chat_id_env": "TELEGRAM_CHAT_ID",
        }
    ],
    "qlir-tradable-human": [
        {
            "adapter": "telegram",
            "bot_token_env": "TRADABLE_HUMAN_TELEGRAM_BOT_TOKEN",
            "chat_id_env": "TELEGRAM_CHAT_ID",
        }
    ],
    "qlir-positioning": [
        {
            "adapter": "telegram",
            "bot_token_env": "POSITIONING_TELEGRAM_BOT_TOKEN",
            "chat_id_env": "TELEGRAM_CHAT_ID",
        }
    ],
}


# -------------------------------------------------
# Adapter factory + validation
# -------------------------------------------------

def build_adapter(spec: dict[str, str]) -> NotificationAdapter:
    adapter_type = spec.get("adapter")

    if adapter_type == "telegram":
        bot_env = spec.get("bot_token_env")
        chat_env = spec.get("chat_id_env")

        if not bot_env or not chat_env:
            raise RuntimeError(
                f"telegram adapter requires 'bot_token_env' and 'chat_id_env': {spec}"
            )

        if bot_env not in os.environ:
            raise RuntimeError(f"missing required env var: {bot_env}")

        if chat_env not in os.environ:
            raise RuntimeError(f"missing required env var: {chat_env}")

        return TelegramAdapter(
            bot_token=os.environ[bot_env],
            chat_id=os.environ[chat_env],
        )

    raise RuntimeError(f"unknown adapter type: {adapter_type}")


def build_outbox_adapters() -> dict[str, list[NotificationAdapter]]:
    """
    Build and validate adapters for all routed outboxes.
    """
    out: dict[str, list[NotificationAdapter]] = {}

    for outbox, specs in OUTBOX_ROUTES.items():
        adapters: list[NotificationAdapter] = []
        for spec in specs:
            adapters.append(build_adapter(spec))
        out[outbox] = adapters

    return out


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def has_outbox_dirs(alerts_root: Path, outbox_routes_keys: list[str]):
    for route in outbox_routes_keys:
        uri = Path(alerts_root / route)
        if not uri.is_dir():
            logger.info(f"Outbox directory not found at: {uri.absolute()}") 

def has_root_outbox_dir(alerts_root: Path) -> bool:
    return any(
        d.is_dir() and not d.name.startswith("_")
        for d in alerts_root.iterdir()
    )


def iter_outbox_dirs() -> Iterable[Path]:
    for d in ALERTS_ROOT.iterdir():
        if not d.is_dir():
            logger.debug(f"Skipping: item is file (we are only iterating dirs): {d}")
            continue
        if d.name.startswith("_") or d.name.startswith("__"):
            logger.debug(f"Skipping: dir name starting with _ or __ : {d}")
            continue
        yield d


def load_alert(path: Path) -> dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def increment_retry(alert: dict[str, Any]) -> None:
    meta = alert.setdefault("_meta", {})
    meta["retries"] = meta.get("retries", 0) + 1


def retries_exceeded(alert: dict[str, Any]) -> bool:
    return alert.get("_meta", {}).get("retries", 0) >= MAX_RETRIES


# -------------------------------------------------
# Main loop
# -------------------------------------------------

logger = setup_logging()

def main() -> None:
    
    ALERTS_ROOT.mkdir(parents=True, exist_ok=True)
    SENT_ROOT.mkdir(parents=True, exist_ok=True)
    FAILED_ROOT.mkdir(parents=True, exist_ok=True)

    logger.info(
        "notification server started (alerts root: %s)", ALERTS_ROOT
    )
    logger.info(
        "TODO: WHEN WE CREATE THE BOT TRADER: Work on Perf... it takes a few hundred ms between each alert send... need to figure out where this massive delay is coming from..."
    )
    logger.info(
        "TODO: Use a logger similar to the data_server and agg_server  found in the example project with all the color formatting"
    )

    # Build + validate all adapters at startup
    outbox_adapters = build_outbox_adapters()
    logger.info("configured outbox routes")
    logger.info(outbox_adapters['qlir-ops'])
    
    has_outbox_dirs(ALERTS_ROOT, outbox_routes_keys=list(outbox_adapters.keys()))

    warned_unrouted: set[str] = set()

    while True:
        if not has_root_outbox_dir(ALERTS_ROOT):
            raise FileNotFoundError(f"No outbox dirs found in: {ALERTS_ROOT}")

        for outbox in iter_outbox_dirs():
            outbox_name = outbox.name

            adapters = outbox_adapters.get(outbox_name)
            if not adapters:
                if outbox_name not in warned_unrouted:
                    logger.warning(
                        "no adapters configured for outbox '%s'; skipping",
                        outbox_name,
                    )
                    warned_unrouted.add(outbox_name)
                continue

            sent_dir = SENT_ROOT / outbox_name
            failed_dir = FAILED_ROOT / outbox_name
            sent_dir.mkdir(parents=True, exist_ok=True)
            failed_dir.mkdir(parents=True, exist_ok=True)
            
            logger.debug(f"checking {outbox_name}")
            
            for alert_path in sorted(outbox.glob("*.json")):
                try:
                    alert = load_alert(alert_path)

                    if "ts" not in alert or "data" not in alert:
                        raise ValueError("invalid alert contract")

                    for adapter in adapters:
                        adapter.send(alert["data"])

                    shutil.move(alert_path, sent_dir / alert_path.name)
                    logger.info(
                        "sent alert %s (outbox=%s)",
                        alert_path.name,
                        outbox_name,
                    )

                except Exception as e:
                    logger.warning(
                        "failed alert %s (outbox=%s): %s",
                        alert_path.name,
                        outbox_name,
                        e,
                    )

                    try:
                        alert = load_alert(alert_path)
                        increment_retry(alert)

                        if retries_exceeded(alert):
                            shutil.move(
                                alert_path, failed_dir / alert_path.name
                            )
                            logger.error(
                                "alert %s moved to failed (outbox=%s)",
                                alert_path.name,
                                outbox_name,
                            )
                        else:
                            with alert_path.open("w") as f:
                                json.dump(alert, f)

                    except Exception as inner:
                        logger.error(
                            "failed to update retry metadata for %s: %s",
                            alert_path.name,
                            inner,
                        )

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
