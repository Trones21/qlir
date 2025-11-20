import pytest

from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit


@pytest.mark.parametrize(
    "count, unit, expected",
    [
        (1, TimeUnit.SECOND, "1s"),
        (5, TimeUnit.MINUTE, "5m"),
        (1, TimeUnit.HOUR,   "1h"),
        (3, TimeUnit.DAY,    "3D"),
    ],
)
def test_to_canonical_resolution_str(count, unit, expected):
    tf = TimeFreq(count=count, unit=unit)
    assert tf.to_canonical_resolution_str() == expected


@pytest.mark.parametrize(
    "s, expected_count, expected_unit",
    [
        ("1s", 1, TimeUnit.SECOND),
        ("5m", 5, TimeUnit.MINUTE),
        ("1h", 1, TimeUnit.HOUR),
        ("3D", 3, TimeUnit.DAY),
    ],
)
def test_from_canonical_resolution_str_roundtrip(s, expected_count, expected_unit):
    tf = TimeFreq.from_canonical_resolution_str(s)
    assert tf.count == expected_count
    assert tf.unit is expected_unit
    # round-trip: to_canonical_resolution_str -> from_ -> to_
    assert tf.to_canonical_resolution_str() == s


@pytest.mark.parametrize("bad", ["", "m", "10", "xs", "10X"])
def test_from_canonical_resolution_str_rejects_invalid(bad):
    with pytest.raises(ValueError):
        TimeFreq.from_canonical_resolution_str(bad)
