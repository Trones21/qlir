
from dataclasses import dataclass
from typing import Callable

from qlir.time.count_intervals import count_intervals_exact, count_intervals_whole
from qlir.time.timeunit import TimeUnit
from qlir.utils.time.fmt import format_delta_ms, format_ts_ms_and_human


@dataclass
class TsDeltaResult:
    a_ms: int
    b_ms: int
    delta_ms: int
    unit: TimeUnit
    delta_units: int
    delta_units_exact: float

    def __str__(self) -> str:
        sign = "+" if self.delta_ms >= 0 else "-"
        direction = "after" if self.delta_ms > 0 else "before"

        exact = abs(self.delta_units_exact)
        whole = abs(self.delta_units)

        return (
            f"[Δ @ {self.unit.value}]\n"
            f"  A: {format_ts_ms_and_human(self.a_ms)}\n"
            f"  B: {format_ts_ms_and_human(self.b_ms)}\n"
            f"  Δ: {sign}{format_delta_ms(abs(self.delta_ms))}\n"
            f"     = {whole} {self.unit.value}{'s' if whole != 1 else ''}\n"
            f"     = {exact:.4f} {self.unit.value}s ({direction})\n"
        )


def compute_ts_delta(unix_a: int, unix_b: int, unit: TimeUnit) -> TsDeltaResult:
    delta_ms = unix_b - unix_a

    return TsDeltaResult(
        a_ms=unix_a,
        b_ms=unix_b,
        delta_ms=delta_ms,
        unit=unit,
        delta_units=count_intervals_whole(delta_ms, unit),
        delta_units_exact=count_intervals_exact(delta_ms, unit),
    )

@dataclass
class TsCompareResult:
    label: str
    a_ms: int
    b_ms: int
    delta_ms: int
    equal: bool

    def __str__(self) -> str:
        eq_flag = "✅" if self.equal else "❌"
        return (
            f"[{eq_flag}] {self.label}\n"
            f"  A: {format_ts_ms_and_human(self.a_ms)}\n"
            f"  B: {format_ts_ms_and_human(self.b_ms)}\n"
            f"  Δ: {format_delta_ms(self.delta_ms)}\n"
        )


def log_ts_compare(
    label: str,
    a_ms: int,
    b_ms: int,
    *,
    logger: Callable[[str], None] = print,
) -> TsCompareResult:
    delta = b_ms - a_ms
    result = TsCompareResult(
        label=label,
        a_ms=a_ms,
        b_ms=b_ms,
        delta_ms=delta,
        equal=(delta == 0),
    )
    logger(str(result))
    return result


def assert_ts_compare(
    label: str,
    a_ms: int,
    b_ms: int,
    *,
    logger: Callable[[str], None] = print,
    assert_equal: bool = False,
) -> TsCompareResult:
    delta = b_ms - a_ms
    result = TsCompareResult(
        label=label,
        a_ms=a_ms,
        b_ms=b_ms,
        delta_ms=delta,
        equal=(delta == 0),
    )
    logger(str(result))
    if assert_equal and not result.equal:
        raise AssertionError(f"{label}: timestamps differ: {delta} ms")
    return result
