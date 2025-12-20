
from dataclasses import dataclass
from typing import Callable

from qlir.utils.time.fmt import format_delta_ms, format_ts_ms


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
            f"  A: {format_ts_ms(self.a_ms)}\n"
            f"  B: {format_ts_ms(self.b_ms)}\n"
            f"  Δ: {format_delta_ms(self.delta_ms)}\n"
        )


def log_ts_compare(
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
