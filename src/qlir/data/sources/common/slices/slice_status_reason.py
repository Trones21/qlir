from enum import Enum


class SliceStatusReason(str, Enum):
    NONE = "none"  # normal fast-path completion

    # Retryable
    TRUNCATED_RESPONSE = "truncated_response"
    BOUNDARY_MISMATCH = "boundary_mismatch"
    HTTP_ERROR = "http_error"
    PARSE_ERROR = "parse_error"

    # Non-retryable (terminal)
    INTERNAL_GAP = "internal_gap"
    HISTORICAL_SPARSITY = "historical_sparsity"
    NO_TRADES = "no_trades"

    # Temporal
    STILL_FORMING = "still_forming"
    TIME_GATED = "time_gated"
