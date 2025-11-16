# Compare a list of venue IDs against QLIR's current symbol-map support.

from typing import Any, Type
from qlir.data.sources.base_symbol_map import BaseSymbolMap


def diff_qlir_support_for_venue_ids(
    symbol_map: Type[BaseSymbolMap],
    venue_ids: list[Any],
):
    """
    Compare a list of venue IDs against QLIR's current symbol-map support.

    Here, "supported" has a very specific meaning:

        QLIR supports a venue ID if and only if there is a defined mapping
        between that venue ID and a CanonicalInstrument in the given
        `symbol_map` (i.e. it appears in symbol_map.all_venue()).

    This does NOT guarantee that:
        - historical data is available on disk,
        - pipelines have been tested for this instrument, or
        - any particular strategy/research code handles it.

    It ONLY asserts that QLIR knows how to map:
        CanonicalInstrument <-> venue ID
    for that symbol map implementation.

    Args:
        symbol_map:
            A subclass of BaseSymbolMap (e.g. DriftSymbolMap, HeliusSymbolMap).
        venue_ids:
            A list of venue-native identifiers fetched from the exchange/API.

    Returns:
        supported:
            Venue IDs that QLIR currently has a CanonicalInstrument mapping for.
        unsupported:
            Venue IDs that are listed by the venue but have no mapping in QLIR.
    """
    supported_set = set(symbol_map.all_venue())

    supported = [vid for vid in venue_ids if vid in supported_set]
    unsupported = [vid for vid in venue_ids if vid not in supported_set]

    return supported, unsupported
