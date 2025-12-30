from enum import Enum

class SliceStatusReason(str, Enum):
    NONE = "none"  # normal fast-path completion

    # Retryable
    HTTP_ERROR = "http_error"
    EXCEPTION = "parse_error"

    # Non-retryable (terminal)
    HISTORICAL_SPARSITY = "historical_sparsity"

    # Temporal
    AWAITING_UPSTREAM = "awaiting_upstream" # The slice should be complete by time, but the upstream system hasn’t delivered all data yet.
    STILL_FORMING = "still_forming"  # The slice is mathematically impossible to be complete yet.