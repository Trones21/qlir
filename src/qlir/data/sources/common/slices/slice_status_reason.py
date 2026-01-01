from enum import StrEnum, auto

class SliceStatusReason(StrEnum):
    NONE = auto()  # normal fast-path completion
    
    # Retryable
    HTTP_ERROR = auto()
    NETWORK_UNAVAILABLE = auto()
    EXCEPTION = auto()


    # Non-retryable (terminal)
    HISTORICAL_SPARSITY = auto()
    NO_DATA = auto()

    # Temporal
    AWAITING_UPSTREAM = auto() # The slice should be complete by time, but the upstream system hasn’t delivered all data yet.
    STILL_FORMING = auto()  # The slice is mathematically impossible to be complete yet.