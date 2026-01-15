# filters.py
import logging


class HasTagFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return hasattr(record, "tag")

class NoTagFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not hasattr(record, "tag")