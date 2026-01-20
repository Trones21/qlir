from typing import Iterable, Literal, overload

from qlir.core.registries.columns.lifecycle import ColumnLifecycleEvent
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.semantics.events import log_column_event

@overload
def announce_column_lifecycle(
    *,
    caller: str,
    decl: ColKeyDecl,
    event: Literal["created", "dropped"],
    reason: str | None = None,
    registry: ColRegistry | None = None,
) -> None: ...

@overload
def announce_column_lifecycle(
    *,
    caller: str,
    registry: ColRegistry,
    decls: Iterable[ColKeyDecl],
    event: Literal["created"],
    reason: str | None = None,
    
) -> None: ...

@overload
def announce_column_lifecycle(
    *,
    caller: str,
    col: str,
    event: Literal["dropped"],
    reason: str | None = None,
) -> None: ...


def announce_column_lifecycle(
    *,
    caller: str,
    decl: ColKeyDecl | None = None,
    decls: Iterable[ColKeyDecl] | None = None,
    col: str | None = None,
    event: Literal["created", "dropped"],
    reason: str | None = None,
    registry: ColRegistry | None = None,
) -> None:
    """
    Announce a column lifecycle event for observability and downstream semantics.

    This helper records human-readable lifecycle events (e.g. CREATED, DROPPED)
    for derived columns and, when applicable, registers key→column bindings
    for downstream pipeline consumers.

    The column(s) are assumed to already exist (or have already been removed)
    at the DataFrame level. This function operates purely in the control plane:
    logging intent and updating semantic registries.

    Parameters
    ----------
    caller : str
        Logical name of the operation emitting the event (e.g. "excursion").

    decl : ColKeyDecl, optional
        A single semantic column declaration (key + concrete column name).

    decls : Iterable[ColKeyDecl], optional
        Multiple semantic column declarations. Each declaration is announced
        independently. If a `reason` is provided, it applies uniformly to all
        declarations in this call.

    col : str, optional
        A non-semantic or aggregate column label (e.g. "MANY (FILTER)",
        "kept_cols", "N/A"). Intended for events where no stable key→column
        binding exists.

    event : {"created", "dropped"}
        Lifecycle transition being announced.

    reason : str, optional
        Additional context explaining why the event occurred. When multiple
        declarations are provided, the same reason is applied to all emitted
        events.

    registry : ColRegistry, optional
        Registry to update with key→column bindings. Registry updates occur
        only for CREATED events with concrete declarations.

    Notes
    -----
    - This function does not mutate DataFrames.
    - Lifecycle events are emitted even when no registry update occurs.
    - Aggregate or non-addressable columns should be passed via `col`
      with `key` implicitly treated as "N/A".

    Examples
    --------
    Single column creation:

    >>> announce_column_lifecycle(
    ...     caller="excursion",
    ...     decl=ColKeyDecl("excursion_bps", "osma14_up_mae_exc_bps"),
    ...     event="created",
    ...     registry=new_cols,
    ... )

    Multiple columns created in one call:

    >>> announce_column_lifecycle(
    ...     caller="foo",
    ...     decls=[
    ...         ColKeyDecl("a", "col_a"),
    ...         ColKeyDecl("b", "col_b"),
    ...     ],
    ...     event="created",
    ...     reason="derived together",
    ...     registry=new_cols,
    ... )

    Many columns dropped via filter:

    >>> announce_column_lifecycle(
    ...     caller="excursion",
    ...     col="MANY (FILTER)",
    ...     event="dropped",
    ...     reason="post-filter cleanup",
    ... )
    """
    if decl is not None:
        decls_iter = (decl,)
    elif decls is not None:
        decls_iter = tuple(decls)
    elif col is not None:
        # sentinel / aggregated case
        log_column_event(
            caller=caller,
            ev=ColumnLifecycleEvent(
                key="N/A",
                col=col,
                event=event,
                reason=reason,
            ),
        )
        return
    else:
        raise ValueError("One of decl, decls, or col must be provided")

    for d in decls_iter:
        log_column_event(
            caller=caller,
            ev=ColumnLifecycleEvent(
                key=d.key,
                col=d.column,
                event=event,
                reason=reason,
            ),
        )

        if registry is not None and event == "created":
            registry.add(key=d.key, column=d.column)
