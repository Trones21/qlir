from pathlib import Path
from typing import Optional


def detect_homogeneous_filetype(folder: str | Path) -> Optional[str]:
    """
    Check whether all files in a folder have the same extension.
    Returns the common extension (without the dot) if homogeneous,
    otherwise None.
    """
    folder = Path(folder)
    exts = {
        f.suffix.lower().lstrip(".")
        for f in folder.iterdir()
        if f.is_file()
    }

    if not exts:
        return None  # no files
    if len(exts) == 1:
        return exts.pop()  # return the single extension, e.g. 'parquet'
    return None  # mixed extensions


def has_files(path: Path) -> bool:
    """
    Return True if `path` exists and contains at least one file.
    Works for directories or single file paths.
    """
    if not path.exists():
        return False

    if path.is_file():
        return True

    if not path.is_dir():
        return False

    # Early-exit: stop on first entry
    try:
        next(path.iterdir())
        return True
    except StopIteration:
        return False
