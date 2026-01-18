import pytest

from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import isolate_open_time_from_composite_key, isolate_open_time_from_request_url
from qlir.data.sources.binance.endpoints.klines.manifest.validation.violations import ManifestViolation

#SLice Key format f"{self.symbol}:{self.interval}:{self.start_ms}:{self.limit}"

def test_isolate_open_time_from_request_url_happy_path():
    slice_key = "BTCUSDT:1m:1503122400000:1000"
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

    open_ts = isolate_open_time_from_request_url(slice_key, slice_entry)

    assert open_ts == 1503122400000


def test_isolate_open_time_from_request_url_missing_requested_url():
    slice_entry = {}

    res = isolate_open_time_from_request_url(slice_key="", slice_entry=slice_entry)
    assert type(res) == ManifestViolation
    assert res.rule == "missing_requested_url_or_url"

def test_isolate_open_time_from_request_url_missing_starttime():
    slice_key = "N/A"
    slice_entry = {
        "requested_url": (
            "https://api.binance.com/api/v3/klines"
            "?symbol=BTCUSDT"
            "&interval=1m"
            "&limit=1000"
        )
    }

    res = isolate_open_time_from_request_url(slice_key, slice_entry)
    assert type(res) == ManifestViolation
    assert res.rule == "missing_startTime"

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

    res = isolate_open_time_from_request_url(slice_key="N/A", slice_entry=slice_entry)
    assert type(res) == ManifestViolation
    assert res.rule == "invalid_startTime"

def test_isolate_open_time_from_composite_key_happy_path():
    composite_key = "BTCUSDT:1m:1503122400000:1000"

    open_ts = isolate_open_time_from_composite_key(composite_key)

    assert open_ts == 1503122400000


def test_isolate_open_time_from_composite_key_invalid_format():
    composite_key = "BTCUSDT:1m"
    slice_entry = {}

    res = isolate_open_time_from_composite_key(composite_key)
    assert type(res) == ManifestViolation
    assert res.rule == "composite_key_format_error"
    
def test_isolate_open_time_from_composite_key_invalid_starttime():
    composite_key = "BTCUSDT:1m:not_an_int:1000"
    slice_entry = {}

    res = isolate_open_time_from_composite_key(composite_key)
    assert type(res) == ManifestViolation
    assert res.message.startswith("Invalid startTime")
