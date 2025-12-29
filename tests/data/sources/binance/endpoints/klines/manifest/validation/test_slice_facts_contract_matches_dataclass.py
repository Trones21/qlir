import dataclasses
from qlir.data.sources.binance.endpoints.klines.manifest.validation.contracts.slice_facts_parts import SliceFactsParts
from qlir.data.sources.binance.endpoints.klines.manifest.validation.slice_facts import SliceFacts


def test_slice_facts_contract_matches_dataclass():
    parts_keys = set(SliceFactsParts.__annotations__.keys())
    dataclass_keys = {
        f.name for f in dataclasses.fields(SliceFacts)
    }

    assert parts_keys == dataclass_keys