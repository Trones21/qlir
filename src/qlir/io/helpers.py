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
