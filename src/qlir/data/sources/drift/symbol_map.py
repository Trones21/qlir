# qlir/data/sources/drift/symbol_map.py

"""
DriftSymbolMap
==============

This class defines Drift's instrument → venue-ID mappings.

Important:
    - All behavior (to_venue, to_canonical, reverse_map, all_*) is implemented
      in BaseSymbolMap.
    - This subclass only provides the `FORWARD_MAP`.
    - If Drift ever requires custom mapping logic (e.g., structured IDs),
      override the relevant methods here.

See:
    qlir.data.sources.base_symbol_map.BaseSymbolMap
"""

from qlir.data.core.instruments import CanonicalInstrument
from qlir.data.sources.base_symbol_map import BaseSymbolMap


class DriftSymbolMap(BaseSymbolMap):
    """Venue-ID mapping for the Drift exchange."""

    FORWARD_MAP = {
        CanonicalInstrument.SOL_PERP: "SOL-PERP",
        CanonicalInstrument.BTC_PERP: "BTC-PERP",
        CanonicalInstrument.ETH_PERP: "ETH-PERP",
        CanonicalInstrument.BONK_PERP: "BONK-PERP",
        CanonicalInstrument.DOGE_PERP: "DOGE-PERP",
    }