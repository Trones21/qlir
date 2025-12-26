# afterdata/logging/colors.py
from qlir.utils.str.color import Ansi, colorize  # or wherever yours lives
import logging

LEVEL_COLOR = {
    logging.DEBUG: Ansi.DIM,
    logging.INFO: Ansi.CYAN,
    logging.WARNING: Ansi.YELLOW,
    logging.ERROR: Ansi.RED,
    logging.CRITICAL: Ansi.RED + Ansi.BOLD,
}
