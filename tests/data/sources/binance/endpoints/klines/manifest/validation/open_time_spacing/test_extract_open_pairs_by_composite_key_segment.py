from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import extract_open_pairs_by_composite_key_segment


def test_extract_open_pairs_by_composite_key_segment():
    slices = {
        "BTCUSDT:1m:1503122400000:1000": {
            "requested_url": "dummy",
        },
        "BTCUSDT:1m:1503182400000:1000": {
            "requested_url": "dummy",
        },
    }

    pairs, _ = extract_open_pairs_by_composite_key_segment(slices)

    assert pairs == [
        (1503122400000, slices["BTCUSDT:1m:1503122400000:1000"]),
        (1503182400000, slices["BTCUSDT:1m:1503182400000:1000"]),
    ]
