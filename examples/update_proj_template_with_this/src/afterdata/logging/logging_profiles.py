from enum import Enum


class LogProfile(str, Enum):
    PROD = "prod"             # root=WARNING, qlir=WARNING
    ALL_INFO = "all-info"     # root=INFO,    qlir=INFO
    ALL_DEBUG = "all-debug"   # root=DEBUG,   qlir=DEBUG
    QLIR_INFO = "qlir-info"   # root=WARNING, qlir=INFO
    QLIR_DEBUG = "qlir-debug" # root=WARNING, qlir=DEBUG
