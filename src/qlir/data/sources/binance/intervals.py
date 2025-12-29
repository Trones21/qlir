import re


_INTERVAL_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[sm])$")

def interval_to_ms(interval: str) -> int:
    """
    Convert an interval string to milliseconds.

    Supported formats:
      - <int>s  (seconds)
      - <int>m  (minutes)

    Examples:
      - "5s" -> 5000
      - "2m" -> 120000

    Raises:
        ValueError: If the interval format is invalid.
    """
    if not isinstance(interval, str):
        raise TypeError(f"interval must be str, got {type(interval).__name__}")

    match = _INTERVAL_RE.match(interval.strip())
    if not match:
        raise ValueError(
            f"Invalid interval format: {interval!r} "
            "(expected '<int>s' or '<int>m')"
        )

    value = int(match.group("value"))
    unit = match.group("unit")

    if unit == "s":
        return value * 1_000
    if unit == "m":
        return value * 60_000

    # defensive: regex already constrains this
    raise ValueError(f"Unsupported interval unit: {unit}")
