# analysis_server/emit/outboxes/load.py

from importlib import import_module
from pathlib import Path

OUTBOXES_DIR = Path(__file__).parent


def load_outboxes():
    outboxes = {}

    for d in OUTBOXES_DIR.iterdir():
        if not d.is_dir() or d.name.startswith("__"):
            continue

        mod_base = f"{__package__}.{d.name}"

        trigger_registry = import_module(f"{mod_base}.trigger_registry").TRIGGER_REGISTRY
        active_triggers = import_module(f"{mod_base}.active_triggers").ACTIVE_TRIGGERS

        outboxes[d.name] = {
            "trigger_registry": trigger_registry,
            "active_triggers": active_triggers,
        }

    return outboxes
