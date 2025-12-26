# afterdata/logging/formatters.py
import logging
from afterdata.logging.colors import LEVEL_COLOR
from qlir.utils.str.color import colorize

class ColorizingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        level_color = LEVEL_COLOR.get(record.levelno)
        if level_color:
            record.levelname = colorize(record.levelname, level_color)
        return super().format(record)
