from qlir.perf.memory_event import MemoryEvent, fmt_bytes


def memory_event_debug_str(ev: MemoryEvent) -> str:
    return (
        f"[mem] {ev.label or 'event'} | "
        f"df: {fmt_bytes(ev.df_bytes_before)} → {fmt_bytes(ev.df_bytes_after)} "
        f"(Δ {fmt_bytes(ev.df_delta_bytes)}) | "
        f"rss: {fmt_bytes(ev.rss_before)} → {fmt_bytes(ev.rss_after)} "
        f"(Δ {fmt_bytes(ev.rss_delta_bytes)}) | "
        f"{ev.elapsed_s:.3f}s"
    )


def memory_event_info_str(ev: MemoryEvent) -> str:
    return (
        f"[mem] {ev.label or 'event'} | "
        f"rss Δ {fmt_bytes(ev.rss_delta_bytes)} "
        f"(df Δ {fmt_bytes(ev.df_delta_bytes)})"
    )


def log_memory_debug(ev: MemoryEvent, *, log):
    if log.isEnabledFor(log.DEBUG):
        log.debug(memory_event_debug_str(ev))


def log_memory_info(ev: MemoryEvent, *, log):
    if log.isEnabledFor(log.INFO):
        log.info(memory_event_info_str(ev))