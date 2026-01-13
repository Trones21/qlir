import pytest
import time

from qlir.data.sources.binance.generate_urls import interval_to_ms
from qlir.data.sources.common.slices.slice_classification import try_resolve_current_slice
from qlir.data.sources.common.slices.slice_key import SliceKey

def make_slice(start_ms: int, *, interval="1m", limit=1000) -> SliceKey:
    interval_ms = interval_to_ms(interval)
    span = interval_ms * limit
    return SliceKey(
        symbol="SOLUSDT",
        interval=interval,
        start_ms=start_ms,
        end_ms=start_ms + span - 1,
        limit=limit,
    )



def canon(sk: SliceKey) -> str:
    return sk.canonical_slice_composite_key()


# Happy Path
def test_try_resolve_current_slice_success(monkeypatch):
    interval = "1m"
    limit = 1000
    interval_ms = interval_to_ms(interval)
    span = interval_ms * limit

    # Anchor lattice at t0
    t0 = 1_700_000_000_000  # arbitrary fixed epoch ms
    s0 = make_slice(t0, interval=interval, limit=limit)
    s1 = make_slice(t0 + span, interval=interval, limit=limit)
    s2 = make_slice(t0 + 2 * span, interval=interval, limit=limit)

    expected = [s0, s1, s2]
    slices = {
        canon(s0): {},
        canon(s1): {},
        canon(s2): {},
    }

    # Freeze time inside s2
    now_ms = t0 + 2 * span + span // 2
    monkeypatch.setattr(
        time,
        "time",
        lambda: now_ms / 1000,
    )

    current, expected_iter = try_resolve_current_slice(
        expected=expected,
        slices=slices,
    )

    assert current is not None
    assert current.start_ms == s2.start_ms

    remaining = list(expected_iter)
    assert {sk.start_ms for sk in remaining} == {
        s0.start_ms,
        s1.start_ms,
    }



def test_try_resolve_current_slice_same_slice(monkeypatch):
    interval = "1m"
    limit = 1000
    interval_ms = interval_to_ms(interval)
    span = interval_ms * limit

    t0 = 1_700_000_000_000
    s0 = make_slice(t0, interval=interval, limit=limit)

    expected = [s0]
    slices = {canon(s0): {}}

    # Freeze time inside the same slice
    now_ms = t0 + span // 4
    monkeypatch.setattr(
        time,
        "time",
        lambda: now_ms / 1000,
    )

    current, expected_iter = try_resolve_current_slice(
        expected=expected,
        slices=slices,
    )

    assert current is not None
    assert current.start_ms == s0.start_ms
    assert list(expected_iter) == []



def test_try_resolve_current_slice_missing_in_slices(monkeypatch):
    interval = "1m"
    limit = 1000
    interval_ms = interval_to_ms(interval)
    span = interval_ms * limit

    t0 = 1_700_000_000_000
    s0 = make_slice(t0, interval=interval, limit=limit)

    expected = [s0]
    slices = {canon(s0): {}}

    # Freeze time into *next* slice, which does NOT exist
    now_ms = t0 + span + 1
    monkeypatch.setattr(
        time,
        "time",
        lambda: now_ms / 1000,
    )

    current, expected_iter = try_resolve_current_slice(
        expected=expected,
        slices=slices,
    )

    assert current is None
    assert list(expected_iter) == expected



def test_try_resolve_current_slice_empty_slices():
    expected = [make_slice(1_700_000_000_000)]

    current, expected_iter = try_resolve_current_slice(
        expected=expected,
        slices={},
    )

    assert current is None
    assert list(expected_iter) == expected

def test_try_resolve_current_slice_type_safety(monkeypatch):
    t0 = 1_700_000_000_000
    s0 = make_slice(t0)

    monkeypatch.setattr(time, "time", lambda: (t0 + 1) / 1000)

    current, expected_iter = try_resolve_current_slice(
        expected=[s0],
        slices={canon(s0): {}},
    )

    for sk in expected_iter:
        assert isinstance(sk, SliceKey)
