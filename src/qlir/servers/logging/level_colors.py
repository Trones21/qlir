# qlir.servers/logging/colors.py
import logging

from qlir.utils.str.color import DEBUG_STYLE, Ansi

LEVEL_COLOR = {
    logging.DEBUG: DEBUG_STYLE,
    logging.INFO: Ansi.RESET,
    logging.WARNING: Ansi.YELLOW,
    logging.ERROR: Ansi.RED,
    logging.CRITICAL: Ansi.RED + Ansi.BOLD,
}