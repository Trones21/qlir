from enum import Enum

class DataSource(str, Enum):
    DRIFT = "drift"
    HELIUS = "helius"
    KAIKO = "kaiko"
