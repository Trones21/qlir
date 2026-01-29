from __future__ import annotations

import time
from pathlib import Path

from .config import OpsWatcherConfig
from .outbox import emit_event
from .state import WatcherState, get_check_state


# Simple spam control:
# - emit on transitions ok<->fail
# - also re-emit failures at most once per `repeat_fail_seconds`
_REPEAT_FAIL_SECONDS_DEFAULT = 6 * 3600  # every 6h if still failing


def _should_emit(prev_status: str, new_status: str, last_emit_ts: float, *, now: float, repeat_fail_seconds: int) -> bool:
    if prev_status != new_status:
        return True
    if new_status == "fail" and (now - last_emit_ts) >= repeat_fail_seconds:
        return True
    return False


def run_once(cfg: OpsWatcherConfig, state: WatcherState, *, repeat_fail_seconds: int = _REPEAT_FAIL_SECONDS_DEFAULT) -> None:
    now = time.time()
    outbox = cfg.service.emit_outbox

    # --- self check ---
    if cfg.self_check and cfg.self_check.enabled:
        from .checks_process import eval_self_check

        st = get_check_state(state, "self", "self")
        ok, payload = eval_self_check(cfg.self_check)

        new_status = "ok" if ok else "fail"
        if _should_emit(st.status, new_status, st.last_emit_ts, now=now, repeat_fail_seconds=repeat_fail_seconds):
            emit_event(outbox, event=payload)
            st.last_emit_ts = now
        st.status = new_status

    # --- process checks ---
    from .checks_process import eval_process_check

    for pc in cfg.process_checks:
        st = get_check_state(state, "process", pc.name)
        ok, payload = eval_process_check(pc)
        new_status = "ok" if ok else "fail"

        if _should_emit(st.status, new_status, st.last_emit_ts, now=now, repeat_fail_seconds=repeat_fail_seconds):
            # Only emit on anomalies by default (but transitions include recovery)
            emit_event(outbox, event=payload)
            st.last_emit_ts = now
        st.status = new_status

    # --- log growth checks ---
    from .checks_log_growth import eval_log_growth_check

    for lg in cfg.log_growth_checks:
        st = get_check_state(state, "log_growth", lg.name)

        ok, payload, new_base_ts, new_base_sz = eval_log_growth_check(
            lg,
            baseline_ts=st.baseline_ts,
            baseline_size=st.baseline_size,
        )
        st.baseline_ts = new_base_ts
        st.baseline_size = new_base_sz

        new_status = "ok" if ok else "fail"
        if _should_emit(st.status, new_status, st.last_emit_ts, now=now, repeat_fail_seconds=repeat_fail_seconds):
            # You might choose to suppress some info events; this keeps it simple.
            emit_event(outbox, event=payload)
            st.last_emit_ts = now
        st.status = new_status


def run_forever(cfg: OpsWatcherConfig, state: WatcherState) -> None:
    interval = max(5, int(cfg.service.interval_seconds))

    while True:
        t0 = time.time()
        run_once(cfg, state)
        elapsed = time.time() - t0
        sleep_s = max(0.0, interval - elapsed)
        time.sleep(sleep_s)
