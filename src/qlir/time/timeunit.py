from enum import Enum


class TimeUnit(Enum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @property
    def pandas_symbol(self) -> str:
        """Return the pandas frequency symbol for this time unit."""
        return {
            TimeUnit.SECOND: "sec",
            TimeUnit.MINUTE: "min",
            TimeUnit.HOUR: "h",
            TimeUnit.DAY: "D",
        }[self]
