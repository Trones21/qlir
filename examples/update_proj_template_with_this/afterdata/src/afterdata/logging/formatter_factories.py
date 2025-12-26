import logging

from afterdata.logging.formatters import ColorizingFormatter


def make_simple_formatter() -> logging.Formatter:
    return ColorizingFormatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )


def make_tagged_formatter() -> logging.Formatter:
    return ColorizingFormatter(
        "%(asctime)s [%(levelname)s:%(tag)s] %(name)s: %(message)s"
    )
