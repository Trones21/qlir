from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal

AlertKey = Literal[
    "data_stale",
    "runtime_invariant",
]


INITIAL_BACKOFF = 30          # seconds
MAX_BACKOFF = 15 * 60         # 15 minutes
BACKOFF_MULT = 2


@dataclass
class AlertBackoffState:
    key: AlertKey
    last_emitted_at: datetime | None
    backoff_sec: int


def maybe_emit_alert_with_backoff(
    *,
    state: AlertBackoffState,
    condition: bool,
    now: datetime,
    emit: Callable[[], None],
) -> AlertBackoffState:
    # ---- runtime invariants (deliberately strict) ----
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    if state.last_emitted_at is not None:
        if state.last_emitted_at.tzinfo is None:
            raise ValueError("last_emitted_at must be timezone-aware")
        if state.last_emitted_at > now:
            raise ValueError(
                f"last_emitted_at > now (clock regression?) "
                f"last={state.last_emitted_at} now={now}"
            )

    if state.backoff_sec <= 0:
        raise ValueError(f"backoff_sec must be > 0, got {state.backoff_sec}")

    # ---- condition cleared: reset backoff ----
    if not condition:
        return AlertBackoffState(
            key=state.key,
            last_emitted_at=None,
            backoff_sec=INITIAL_BACKOFF,
        )

    # ---- first emission ----
    if state.last_emitted_at is None:
        emit()
        return AlertBackoffState(
            key=state.key,
            last_emitted_at=now,
            backoff_sec=INITIAL_BACKOFF,
        )

    # ---- backoff window ----
    elapsed = (now - state.last_emitted_at).total_seconds()
    if elapsed >= state.backoff_sec:
        emit()
        return AlertBackoffState(
            key=state.key,
            last_emitted_at=now,
            backoff_sec=min(state.backoff_sec * BACKOFF_MULT, MAX_BACKOFF),
        )

    # ---- still backing off ----
    return state
