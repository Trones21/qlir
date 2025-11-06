from enum import StrEnum


class FileType(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"