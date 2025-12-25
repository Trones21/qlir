from qlir.time.ms_per_unit import MS_PER_UNIT
from qlir.time.timeunit import TimeUnit


def truncate_ms(ms: int, unit: TimeUnit) -> int:
    """
    Truncate a unix-ms timestamp to the given unit boundary.
    """
    step = MS_PER_UNIT[unit]
    return (ms // step) * step


def count_intervals_whole(delta_ms: int, unit: TimeUnit) -> int:
    """
    Return how many whole units fit into a delta.
    """
    step = MS_PER_UNIT[unit]
    return abs(delta_ms) // step

def count_intervals_exact(delta_ms: int, unit: TimeUnit) -> float:
    step = MS_PER_UNIT[unit]
    return delta_ms / step
