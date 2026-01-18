from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import extract_open_pairs_by_composite_key_segment

def test_extraction_preserves_input_order():
    '''This prevents future refactors from silently changing responsibilities. (Putting the sort responsibility into the extraction function)'''
    slices = {
        "BTCUSDT:1m:2000:1000": {"requested_url": "...&startTime=2000"},
        "BTCUSDT:1m:1000:1000": {"requested_url": "...&startTime=1000"},
    }

    pairs, _ = extract_open_pairs_by_composite_key_segment(slices)

    assert pairs[0][0] == 2000
    assert pairs[1][0] == 1000
