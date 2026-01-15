from pathlib import Path
import logging 
log = logging.getLogger(__name__)


def delete_file_if_exists(path):
    try:
        Path(path).unlink()
    except FileNotFoundError:
        log.info(f"No need to delete, file not found at {path}")
        pass