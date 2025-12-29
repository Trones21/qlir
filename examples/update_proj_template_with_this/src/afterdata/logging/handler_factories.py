import logging

from afterdata.logging.filters import HasTagFilter
from afterdata.logging.formatter_factories import make_simple_formatter, make_tagged_formatter


def make_simple_handler(level: int) -> logging.Handler:
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(make_simple_formatter())
    return h


def make_tagged_handler(level: int) -> logging.Handler:
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(make_tagged_formatter())
    return h


def make_telemetry_handler(level: int) -> logging.Handler:
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(make_tagged_formatter())
    h.addFilter(HasTagFilter())
    return h
