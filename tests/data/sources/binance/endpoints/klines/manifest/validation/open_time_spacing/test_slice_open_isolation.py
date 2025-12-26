import pytest

from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import isolate_open_time_from_composite_key, isolate_open_time_from_request_url

def test_isolate_open_time_from_request_url_happy_path():
    slice_entry = {
        "requested_url": (
            "https://api.binance.com/api/v3/klines"
            "?symbol=BTCUSDT"
            "&interval=1m"
            "&limit=1000"
            "&startTime=1503122400000"
            "&endTime=1503182399999"
        )
    }

    open_ts, returned_slice = isolate_open_time_from_request_url(slice_entry)

    assert open_ts == 1503122400000
    assert returned_slice is slice_entry





def test_isolate_open_time_from_request_url_missing_requested_url():
    slice_entry = {}

    with pytest.raises(KeyError, match="requested_url"):
        isolate_open_time_from_request_url(slice_entry)


def test_isolate_open_time_from_request_url_missing_starttime():
    slice_entry = {
        "requested_url": (
            "https://api.binance.com/api/v3/klines"
            "?symbol=BTCUSDT"
            "&interval=1m"
            "&limit=1000"
        )
    }

    with pytest.raises(ValueError, match="startTime"):
        isolate_open_time_from_request_url(slice_entry)


def test_isolate_open_time_from_request_url_invalid_starttime():
    slice_entry = {
        "requested_url": (
            "https://api.binance.com/api/v3/klines"
            "?symbol=BTCUSDT"
            "&interval=1m"
            "&limit=1000"
            "&startTime=not_an_int"
        )
    }

    with pytest.raises(ValueError, match="Invalid startTime"):
        isolate_open_time_from_request_url(slice_entry)


def test_isolate_open_time_from_composite_key_happy_path():
    composite_key = "BTCUSDT:1m:1503122400000:1000"
    slice_entry = {"status": "complete"}

    open_ts, returned_slice = isolate_open_time_from_composite_key(
        composite_key,
        slice_entry,
    )

    assert open_ts == 1503122400000
    assert returned_slice is slice_entry


def test_isolate_open_time_from_composite_key_invalid_format():
    composite_key = "BTCUSDT:1m"
    slice_entry = {}

    with pytest.raises(ValueError, match="Invalid composite key"):
        isolate_open_time_from_composite_key(composite_key, slice_entry)


def test_isolate_open_time_from_composite_key_invalid_starttime():
    composite_key = "BTCUSDT:1m:not_an_int:1000"
    slice_entry = {}

    with pytest.raises(ValueError, match="Invalid startTime"):
        isolate_open_time_from_composite_key(composite_key, slice_entry)
