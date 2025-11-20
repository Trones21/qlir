import os
import logging
from enum import Enum


class LogProfile(str, Enum):
    PROD = "prod"             # root=WARNING, qlir=WARNING
    ALL_INFO = "all-info"     # root=INFO,    qlir=INFO
    ALL_DEBUG = "all-debug"   # root=DEBUG,   qlir=DEBUG
    QLIR_INFO = "qlir-info"   # root=WARNING, qlir=INFO
    QLIR_DEBUG = "qlir-debug" # root=WARNING, qlir=DEBUG


def setup_logging(profile: LogProfile) -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # --- decide root + qlir levels ---
    match profile:
        case LogProfile.PROD:
            root_level = logging.WARNING
            qlir_level = logging.WARNING

        case LogProfile.ALL_INFO:
            root_level = logging.INFO
            qlir_level = logging.INFO

        case LogProfile.ALL_DEBUG:
            root_level = logging.DEBUG
            qlir_level = logging.DEBUG

        case LogProfile.QLIR_INFO:
            root_level = logging.WARNING
            qlir_level = logging.INFO

        case LogProfile.QLIR_DEBUG:
            root_level = logging.WARNING
            qlir_level = logging.DEBUG

        case _:
            raise ValueError(f"Unknown LogProfile: {profile}")

    # --- root logger (global) ---
    logging.basicConfig(
        level=root_level,
        format=fmt,
        force=True,  # ensures reconfiguration if needed
    )

    # --- qlir logger tree ---
    qlir_logger = logging.getLogger("qlir")
    qlir_logger.setLevel(qlir_level)
    qlir_logger.propagate = False  # avoid bubbling into root handler

    handler = logging.StreamHandler()
    handler.setLevel(qlir_level)
    handler.setFormatter(logging.Formatter(fmt))

    qlir_logger.handlers.clear()
    qlir_logger.addHandler(handler)
