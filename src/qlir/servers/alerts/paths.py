from pathlib import Path
import os

def get_alerts_root() -> Path:
    try:
        raw = os.environ["QLIR_ALERTS_DIR"]
    except KeyError:
        raise RuntimeError(
            "QLIR_ALERTS_DIR is not set; alerts directory must be configured explicitly"
        )

    return Path(raw).expanduser().resolve()

def registry_path() -> Path:
    return get_alerts_root() / "outboxes.json"

def get_outbox_dir(name: str) -> Path:
    return get_alerts_root() / name

def get_sent_dir() -> Path:
    return get_alerts_root() / "_sent"

def get_failed_dir() -> Path:
    return get_alerts_root() / "_failed"
