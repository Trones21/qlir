import logging

from qlir.servers.logging.formatters import ColorizingBracketFormatter


def make_simple_formatter() -> logging.Formatter:
    return ColorizingBracketFormatter(
        "%(asctime)s [%(bracket)s] %(name)s: %(message)s"
    )


def make_tagged_formatter() -> logging.Formatter:
    return ColorizingBracketFormatter(
        "%(asctime)s [%(bracket)s] %(name)s: %(message)s"
    )
