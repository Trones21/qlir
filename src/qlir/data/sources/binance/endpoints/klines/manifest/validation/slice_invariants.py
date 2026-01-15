from dataclasses import dataclass
import hashlib

from qlir.data.sources.binance.endpoints.klines.manifest.validation.contracts.slice_facts_parts import (
    SliceInvariantsParts,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.parsers.composite_key_parser import (
    parse_composite_slice_key,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.parsers.request_url_parser import (
    parse_requested_kline_url,
)


@dataclass(frozen=True)
class SliceInvariants:
    symbol: str
    interval: str
    limit: int
    start_time: int


def canonical_slice_comp_key_from_facts(facts: SliceInvariants) -> str:
    """
    Canonical composite key derived from SliceFacts.

    Intentionally duplicated from runtime logic to detect drift.
    """
    return f"{facts.symbol}:{facts.interval}:{facts.start_time}:{facts.limit}"


def compute_slice_id_from_facts(facts: SliceInvariants) -> str:
    """
    Compute expected slice_id from SliceFacts.

    Uses the same hash algorithm as runtime, but does NOT depend on
    KlineSliceKey. If construction semantics change upstream, this
    invariant will fail by design.
    """
    key = canonical_slice_comp_key_from_facts(facts).encode("utf-8")
    return hashlib.blake2b(key, digest_size=16).hexdigest()

def extract_facts_from_composite_key(key: str) -> SliceInvariants:
    parts: SliceInvariantsParts = parse_composite_slice_key(key)
    return SliceInvariants(**parts)


def extract_facts_from_manifest(manifest: dict, start_time: int) -> SliceInvariants:
    return SliceInvariants(
        symbol=manifest["symbol"],
        interval=manifest["interval"],
        limit=manifest["limit"],
        start_time=start_time,
    )


def extract_facts_from_requested_url(url: str) -> SliceInvariants:
    parts: SliceInvariantsParts = parse_requested_kline_url(url)
    return SliceInvariants(**parts)
