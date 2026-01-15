from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from typing import List, Optional

from .row_derivation import ColumnDerivationSpec, ColumnLifecycleEvent

_CTX: contextvars.ContextVar["DerivationContext | None"] = contextvars.ContextVar(
    "qlir_derivation_context",
    default=None,
)


@dataclass
class DerivationContext:
    """
    Collects derivation specs and lifecycle events for a pipeline / call-chain.

    This is intentionally lightweight: it's a list collector + emitter.
    """

    specs: List[tuple[str, ColumnDerivationSpec]] = field(default_factory=list)  # (col, spec)
    lifecycle: List[ColumnLifecycleEvent] = field(default_factory=list)

    def add_created(self, *, col: str, spec: ColumnDerivationSpec) -> None:
        self.specs.append((col, spec))
        self.lifecycle.append(ColumnLifecycleEvent(col=col, event="created"))

    def add_dropped(self, *, col: str, reason: Optional[str] = None) -> None:
        self.lifecycle.append(ColumnLifecycleEvent(col=col, event="dropped", reason=reason))

    def created_cols(self) -> list[str]:
        return [e.col for e in self.lifecycle if e.event == "created"]

    def dropped_cols(self) -> list[str]:
        return [e.col for e in self.lifecycle if e.event == "dropped"]


def get_ctx() -> DerivationContext | None:
    return _CTX.get()


class derivation_scope:
    """
    Context manager that installs a DerivationContext for nested calls.

    Usage:
        with derivation_scope() as ctx:
            df, col = sma(...)
            ...
        # ctx now contains specs/events
    """

    def __init__(self, ctx: DerivationContext | None = None):
        self._ctx = ctx or DerivationContext()
        self._token = None

    def __enter__(self) -> DerivationContext:
        self._token = _CTX.set(self._ctx)
        return self._ctx

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _CTX.reset(self._token)