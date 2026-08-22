# analysis_server/emit/outboxes/load.py

from importlib import import_module
from pathlib import Path

OUTBOXES_DIR = Path(__file__).parent


def load_outboxes():
    """
    Discover every outbox package and key the result by its CANONICAL OUTBOX NAME
    (e.g. "qlir-events"), not by its package directory name (e.g. "qlir_events").

    That distinction is the whole point. Package dirs must be valid Python module
    names, so they use underscores; the outbox name is the cross-process identifier
    used for the $QLIR_ALERTS_DIR directory, the analysis_outboxes.json key, and the
    notification server's routing key. Keying this dict by directory name silently
    made every emitted alert land in an underscore directory that no notification
    route matched, so nothing was ever delivered.
    """
    outboxes = {}

    for d in sorted(OUTBOXES_DIR.iterdir()):
        if not d.is_dir() or d.name.startswith("__"):
            continue

        mod_base = f"{__package__}.{d.name}"

        try:
            meta = import_module(f"{mod_base}.meta").OUTBOX
        except ModuleNotFoundError as e:
            raise RuntimeError(
                f"Outbox package '{d.name}' has no meta.py. Every outbox must declare "
                f"its canonical name and alert level. See ALERT_OUTBOXES.md."
            ) from e

        name = meta["name"]

        if name in outboxes:
            raise RuntimeError(
                f"Two outbox packages declare the same outbox name {name!r}; "
                f"names must be unique."
            )

        trigger_registry = import_module(f"{mod_base}.trigger_registry").TRIGGER_REGISTRY
        active_triggers = import_module(f"{mod_base}.active_triggers").ACTIVE_TRIGGERS

        outboxes[name] = {
            "package": d.name,
            "alert_level": meta["alert_level"],
            "priority": meta["priority"],
            "trigger_registry": trigger_registry,
            "active_triggers": active_triggers,
        }

    return outboxes
