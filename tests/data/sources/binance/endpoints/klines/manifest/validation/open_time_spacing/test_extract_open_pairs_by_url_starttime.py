from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import extract_open_pairs_by_url_starttime


def test_extract_open_pairs_by_url_starttime():
    slices = {
        "BTCUSDT:1m:1503122400000:1000": {
            "requested_url":
                "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000&startTime=1503122400000&endTime=1503182399999"
            ,
        },
        "BTCUSDT:1m:1503182400000:1000": {
            "requested_url": 
            "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000&startTime=1503182400000&endTime=1503242399999"
        },
    }

    pairs = extract_open_pairs_by_url_starttime(slices)

    assert pairs == [
        (1503122400000, slices["BTCUSDT:1m:1503122400000:1000"]),
        (1503182400000, slices["BTCUSDT:1m:1503182400000:1000"]),
    ]
