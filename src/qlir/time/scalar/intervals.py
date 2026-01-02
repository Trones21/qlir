def violates_expected_interval(
    prev_ts: int,
    next_ts: int,
    expected_interval_s: int,
) -> bool:
    return (next_ts - prev_ts) != expected_interval_s
