import os
import logging
from enum import Enum

from afterdata.logging.filters import HasTagFilter, NoTagFilter
from afterdata.logging.handler_factories import make_simple_handler, make_tagged_handler, make_telemetry_handler
from afterdata.logging.level_resolution import resolve_levels
from afterdata.logging.logging_profiles import LogProfile


def setup_logging(profile: LogProfile, *, enable_telemetry: bool = False) -> None:
    root_level, qlir_level = resolve_levels(profile)

    # ---- ROOT ----
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(root_level)
    root.addHandler(make_simple_handler(root_level))

    # ---- QLIR (normal logs) ----
    qlir = logging.getLogger("qlir")
    qlir.handlers.clear()
    qlir.setLevel(qlir_level)
    qlir.propagate = False
    simple = make_simple_handler(qlir_level)
    simple.addFilter(NoTagFilter())
    qlir.addHandler(simple)

    # tagged handler (only sees tagged records)
    tagged = make_tagged_handler(qlir_level)
    tagged.addFilter(HasTagFilter())
    qlir.addHandler(tagged)

    # ---- TELEMETRY (scaffolded) ----
    if enable_telemetry:
        tlog = logging.getLogger("qlir.telemetry")
        tlog.handlers.clear()
        tlog.setLevel(qlir_level)
        tlog.propagate = False
        tlog.addHandler(make_telemetry_handler(qlir_level))

    dump_logging_tree("qlir")
    dump_logging_tree()  # root

def dump_logging_tree(prefix: str = "qlir"):
    lg = logging.getLogger(prefix)
    print(f"Logger: {lg.name} level={lg.level} propagate={lg.propagate}")
    for i, h in enumerate(lg.handlers):
        fmt = getattr(h.formatter, "_fmt", None)
        print(f"  handler[{i}]: {type(h).__name__} level={h.level} formatter={type(h.formatter).__name__} fmt={fmt}")













# def setup_logging(profile: LogProfile) -> None:
#         # ---- SIMPLE handler ----
#     simple = logging.StreamHandler()
#     simple.setFormatter(
#         logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
#     )

#     # ---- TAGGED handler (expects tag) ----
#     tagged = logging.StreamHandler()
#     tagged.setFormatter(
#         logging.Formatter("%(asctime)s [%(levelname)s:%(tag)s] %(name)s: %(message)s")
#     )


#     # --- decide root + qlir levels ---
#     match profile:
#         case LogProfile.PROD:
#             root_level = logging.WARNING
#             qlir_level = logging.WARNING

#         case LogProfile.ALL_INFO:
#             root_level = logging.INFO
#             qlir_level = logging.INFO

#         case LogProfile.ALL_DEBUG:
#             root_level = logging.DEBUG
#             qlir_level = logging.DEBUG

#         case LogProfile.QLIR_INFO:
#             root_level = logging.WARNING
#             qlir_level = logging.INFO

#         case LogProfile.QLIR_DEBUG:
#             root_level = logging.WARNING
#             qlir_level = logging.DEBUG

#         case _:
#             raise ValueError(f"Unknown LogProfile: {profile}")


#     # --- root logger (global) ---
#     logging.basicConfig(
#         level=root_level,
#         format=simple,
#         force=True,  # ensures reconfiguration if needed
#     )

#     # --- qlir logger tree ---
#     qlir_logger = logging.getLogger("qlir")
#     qlir_logger.setLevel(qlir_level)
#     qlir_logger.propagate = False  # avoid bubbling into root handler

#     handler = logging.StreamHandler()
#     handler.setLevel(qlir_level)
#     handler.setFormatter(logging.Formatter(fmt))

#     qlir_logger.handlers.clear()
#     qlir_logger.addHandler(handler)
