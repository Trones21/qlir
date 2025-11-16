from enum import Enum


class CanonicalInstrument(Enum):
    SOL_PERP = "SOL_PERP"
    BTC_PERP = "BTC_PERP"
    ETH_PERP = "ETH_PERP"
    BONK_PERP = "BONK_PERP"
    DOGE_PERP = "DOGE_PERP"

    @classmethod
    def from_str(cls, s: str):
        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"Unknown canonical instrument: {s}")

