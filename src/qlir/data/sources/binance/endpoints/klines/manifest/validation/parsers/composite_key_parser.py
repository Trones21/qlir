from qlir.data.sources.binance.endpoints.klines.manifest.validation.contracts.slice_facts_parts import (
    SliceInvariantsParts,
)


class CompositeKeyParseError(ValueError):
    pass


def parse_composite_slice_key(key: str) -> SliceInvariantsParts:
    """
    Parse a composite slice key of the form:

        <symbol>:<interval>:<start_time_ms>:<limit>

    Returns SliceFacts if valid, raises CompositeKeyParseError otherwise.
    """
    parts = key.split(":")

    if len(parts) != 4:
        raise CompositeKeyParseError(
            f"Invalid composite key format (expected 4 parts): {key}"
        )

    symbol, interval, start_str, limit_str = parts

    try:
        start_time = int(start_str)
    except ValueError:
        raise CompositeKeyParseError(
            f"Invalid start_time in composite key: {start_str}"
        )

    try:
        limit = int(limit_str)
    except ValueError:
        raise CompositeKeyParseError(
            f"Invalid limit in composite key: {limit_str}"
        )

    if start_time < 0:
        raise CompositeKeyParseError(
            f"start_time must be non-negative: {start_time}"
        )

    if limit <= 0:
        raise CompositeKeyParseError(
            f"limit must be > 0: {limit}"
        )

    return {
        "symbol": symbol,
        "interval": interval,
        "start_time": start_time,
        "limit": limit,
    }
