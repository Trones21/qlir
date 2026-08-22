import json
import shutil
import time
from pathlib import Path
from typing import Any, Iterable

from .adapters.base import NotificationAdapter
from .adapters.registry import SinkConfigError
from .config import build_adapters, load_config, read_declared_outboxes
from .logging import setup_logging

from qlir.servers.alerts.paths import get_alerts_root

POLL_INTERVAL_SEC = 2.0
MAX_RETRIES = 3

logger = setup_logging()


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def iter_outbox_dirs(alerts_root: Path) -> Iterable[Path]:
    for d in sorted(alerts_root.iterdir()):
        if not d.is_dir():
            continue
        if d.name.startswith("_"):  # _sent / _failed
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


def report_routing(
    outbox_adapters: dict[str, list[NotificationAdapter]],
    declared: dict[str, dict],
) -> None:
    """
    Log what this server will and will not handle, and flag the two ways a
    route can be pointless -- so a silent no-op is always visible at startup.
    """
    for outbox, adapters in sorted(outbox_adapters.items()):
        kinds = ", ".join(type(a).__name__.replace("Adapter", "").lower() for a in adapters)
        logger.info("routing %-26s -> %s", outbox, kinds)

    # routed but nobody declares it: fine for ops_watcher, a typo otherwise
    if declared:
        undeclared = sorted(set(outbox_adapters) - set(declared))
        if undeclared:
            logger.warning(
                "Routed but not declared in analysis_outboxes.json: %s. Expected for "
                "outboxes produced outside the analysis server (e.g. qlir-ops from "
                "ops_watcher); otherwise check for a typo.",
                ", ".join(undeclared),
            )

        # declared but unrouted: alerts will pile up undelivered
        unrouted = sorted(set(declared) - set(outbox_adapters))
        if unrouted:
            logger.warning(
                "Declared by the analysis server but NOT routed here: %s. Alerts written "
                "to these outboxes will accumulate on disk and never be delivered.",
                ", ".join(unrouted),
            )


# -------------------------------------------------
# Main loop
# -------------------------------------------------

def main() -> None:
    alerts_root = get_alerts_root()
    sent_root = alerts_root / "_sent"
    failed_root = alerts_root / "_failed"

    alerts_root.mkdir(parents=True, exist_ok=True)
    sent_root.mkdir(parents=True, exist_ok=True)
    failed_root.mkdir(parents=True, exist_ok=True)

    logger.info("notification server starting (alerts root: %s)", alerts_root)

    declared = read_declared_outboxes(alerts_root)
    if declared:
        logger.info("analysis server declares %d outbox(es): %s",
                    len(declared), ", ".join(sorted(declared)))
    else:
        logger.info(
            "No analysis_outboxes.json yet -- the analysis server has not declared any "
            "outboxes. This is fine; it may not have started."
        )

    try:
        cfg = load_config(declared_outboxes=sorted(declared))
        outbox_adapters = build_adapters(cfg)
    except SinkConfigError as e:
        # A selected channel is unusable. Fail loudly with setup steps rather than
        # starting up and silently dropping alerts.
        logger.error("%s", e)
        raise SystemExit(1)

    logger.info("notification config: %s", cfg.source)
    report_routing(outbox_adapters, declared)

    warned_unrouted: set[str] = set()

    while True:
        for outbox in iter_outbox_dirs(alerts_root):
            outbox_name = outbox.name
            adapters = outbox_adapters.get(outbox_name)

            if not adapters:
                if outbox_name not in warned_unrouted:
                    logger.warning(
                        "outbox '%s' has alerts but no route in %s; leaving them in place",
                        outbox_name, cfg.source,
                    )
                    warned_unrouted.add(outbox_name)
                continue

            sent_dir = sent_root / outbox_name
            failed_dir = failed_root / outbox_name
            sent_dir.mkdir(parents=True, exist_ok=True)
            failed_dir.mkdir(parents=True, exist_ok=True)

            for alert_path in sorted(outbox.glob("*.json")):
                try:
                    alert = load_alert(alert_path)

                    if "ts" not in alert or "data" not in alert:
                        raise ValueError("invalid alert contract")

                    for adapter in adapters:
                        adapter.send(alert["data"])

                    shutil.move(alert_path, sent_dir / alert_path.name)
                    logger.info("sent alert %s (outbox=%s)", alert_path.name, outbox_name)

                except Exception as e:
                    logger.warning(
                        "failed alert %s (outbox=%s): %s", alert_path.name, outbox_name, e
                    )

                    try:
                        alert = load_alert(alert_path)
                        increment_retry(alert)

                        if retries_exceeded(alert):
                            shutil.move(alert_path, failed_dir / alert_path.name)
                            logger.error(
                                "alert %s moved to failed (outbox=%s)",
                                alert_path.name, outbox_name,
                            )
                        else:
                            with alert_path.open("w") as f:
                                json.dump(alert, f)

                    except Exception as inner:
                        logger.error(
                            "failed to update retry metadata for %s: %s",
                            alert_path.name, inner,
                        )

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
