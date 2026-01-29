from qlir.perf.memory_event import MemoryEvent, fmt_bytes
from logging import INFO, DEBUG


# the reason we have different methods ( _debug and _info) is so that we can put those calls in different places 
# and "toggle" the granularity (number of call sites) by simply changing the logging level (during normal analysis sessions you will probably run on info, but if running into OOM issues you may switch to debug) 


# Both of these call the same formatter, but we could always add more or less verbose / clear messages if we choose to  

def log_memory_debug(ev: MemoryEvent, *, log):
    if log.isEnabledFor(DEBUG):
        log.debug(memory_event_str(ev))


def log_memory_info(ev: MemoryEvent, *, log):
    if log.isEnabledFor(INFO):
        log.info(memory_event_str(ev))


def memory_event_str(ev: MemoryEvent) -> str:
    return (
        f"[mem] {ev.label or 'event'} | "
        f"df: {fmt_bytes(ev.df_bytes_before)} → {fmt_bytes(ev.df_bytes_after)} "
        f"(Δ {fmt_bytes(ev.df_delta_bytes)}) | "
        f"rss: {fmt_bytes(ev.rss_before)} → {fmt_bytes(ev.rss_after)} "
        f"(Δ {fmt_bytes(ev.rss_delta_bytes)}) | "
        f"{ev.elapsed_s:.3f}s"
    )


# def memory_event_info_str(ev: MemoryEvent) -> str:
#     return (
#         f"[mem] {ev.label or 'event'} | "
#         f"df: {fmt_bytes(ev.df_bytes_before)} → {fmt_bytes(ev.df_bytes_after)} "
#         f"(Δ {fmt_bytes(ev.df_delta_bytes)}) | "
#         f"rss: {fmt_bytes(ev.rss_before)} → {fmt_bytes(ev.rss_after)} "
#         f"(Δ {fmt_bytes(ev.rss_delta_bytes)}) | "
#         f"{ev.elapsed_s:.3f}s"
#     )

