import logging


def get_telemetry_logger(name: str | None = None) -> logging.Logger:
    base = "qlir.telemetry"
    return logging.getLogger(f"{base}.{name}" if name else base)