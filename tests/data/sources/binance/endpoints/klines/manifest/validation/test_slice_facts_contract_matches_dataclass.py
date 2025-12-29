import dataclasses
from qlir.data.sources.binance.endpoints.klines.manifest.validation.contracts.slice_facts_parts import SliceInvariantsParts
from qlir.data.sources.binance.endpoints.klines.manifest.validation.slice_invariants import SliceInvariants


def test_slice_facts_contract_matches_dataclass():
    parts_keys = set(SliceInvariantsParts.__annotations__.keys())
    dataclass_keys = {
        f.name for f in dataclasses.fields(SliceInvariants)
    }

    assert parts_keys == dataclass_keys