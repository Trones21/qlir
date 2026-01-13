# qlir.servers/logging/colors.py
from qlir.utils.str.color import Ansi, colorize, DEBUG_STYLE
import logging

LEVEL_COLOR = {
    logging.DEBUG: DEBUG_STYLE,
    logging.INFO: Ansi.RESET,
    logging.WARNING: Ansi.YELLOW,
    logging.ERROR: Ansi.RED,
    logging.CRITICAL: Ansi.RED + Ansi.BOLD,
}