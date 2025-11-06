from enum import StrEnum
import time
from pandas import DataFrame
from qlir.io.writer import write
import pyarrow.dataset as ds
from .filetype import FileType
import logging
log = logging.getLogger(__name__)


def write_checkpoint(df: DataFrame, file_type: FileType, static_part_of_pathname: str):
    timestamp = int(time.time() * 1000)
    path = f"{static_part_of_pathname}_{timestamp}.{file_type.value}"
    write(df, path)