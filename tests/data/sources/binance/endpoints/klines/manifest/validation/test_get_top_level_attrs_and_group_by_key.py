from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_structure import get_distinct_top_level_metadata_structures_and_group_by_key


def test_get_distinct_top_level_metadata_structures_and_group_by_key():
    manifest = {
        "slice_a": {
            "__contract": {"status": "ok"},
            "__meta_contract": {"status": "ok"},
            "completed_at": "2025-12-25T16:49:34Z",
            "http_status": 200,
            "n_items": 1000,
            "slice_status": "complete",
        },
        "slice_b": {
            "__contract": {"status": "ok"},
            "__meta_contract": {"status": "ok"},
            "completed_at": "2025-12-25T16:50:10Z",
            "http_status": 200,
            "n_items": 1000,
            "slice_status": "complete",
        },
        # Missing n_items + http_status → different shape
        "slice_c": {
            "__contract": {"status": "ok"},
            "__meta_contract": {"status": "ok"},
            "completed_at": "2025-12-25T16:51:00Z",
            "slice_status": "failed",
            "error": "timeout",
        },
    }

    result = get_distinct_top_level_metadata_structures_and_group_by_key([manifest])

    # Two distinct shapes
    assert len(result) == 2

    # Map keys → slice_keys for easy assertions
    groups = {tuple(r["keys"]): set(r["slice_keys"]) for r in result}

    full_shape_keys = tuple(
        sorted(
            [
                "__contract",
                "__meta_contract",
                "completed_at",
                "http_status",
                "n_items",
                "slice_status",
            ]
        )
    )

    partial_shape_keys = tuple(
        sorted(
            [
                "__contract",
                "__meta_contract",
                "completed_at",
                "slice_status",
                "error",
            ]
        )
    )

    assert groups[full_shape_keys] == {"slice_a", "slice_b"}
    assert groups[partial_shape_keys] == {"slice_c"}
