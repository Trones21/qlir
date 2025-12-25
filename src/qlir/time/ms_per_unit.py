from qlir.time.timeunit import TimeUnit


MS_PER_UNIT: dict[TimeUnit, int] = {
    TimeUnit.SECOND: 1_000,
    TimeUnit.MINUTE: 60_000,
    TimeUnit.HOUR:   3_600_000,
    TimeUnit.DAY:    86_400_000,
}
