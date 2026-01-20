from __future__ import annotations

import functools
import inspect
import logging
from typing import Callable, Mapping, ParamSpec, Union

from qlir.core.types.annotated_df import AnnotatedDF
from qlir.core.semantics.context import get_ctx
from qlir.core.semantics.explain import explain_created
from qlir.core.semantics.col_derivation import ColumnDerivationSpec

P = ParamSpec("P")

Specs = Union[
    ColumnDerivationSpec,                      # single spec
    Mapping[str, ColumnDerivationSpec],        # key -> spec
]
SpecsOrCallable = Union[
    Specs,
    Callable[..., Specs],
]


def _normalize_specs_for_keys(
    resolved: Specs,
    *,
    declared_keys: list[str],
    context: str,
) -> Mapping[str, ColumnDerivationSpec]:
    """
    Normalize specs into a mapping keyed by ColRegistry keys.

    Rules:
    - If resolved is a dict: returned as-is.
    - If resolved is a single spec:
        - If exactly 1 declared key: bind it to that key.
        - If >1 declared keys: raise (forces you to be explicit).
    """
    if isinstance(resolved, Mapping):
        return resolved

    # single spec
    if len(declared_keys) != 1:
        raise ValueError(
            f"{context}: single ColumnDerivationSpec provided, but function declared "
            f"{len(declared_keys)} keys: {declared_keys}. Provide a mapping instead."
        )
    return {declared_keys[0]: resolved}


def new_col_func(
    *,
    specs: SpecsOrCallable,
) -> Callable[[Callable[P, AnnotatedDF]], Callable[P, AnnotatedDF]]:
    """
    Decorator for functions that create new columns.

    Contract:
    - Wrapped function returns AnnotatedDF and declares columns in adf.cols (ColRegistry).
    - `specs` describes row-derivation for created columns:
        * ColumnDerivationSpec (only valid if exactly 1 key is declared), OR
        * Mapping[key, ColumnDerivationSpec]
      You may also pass a callable that returns either form based on bound args.
    """

    def decorator(fn: Callable[P, AnnotatedDF]) -> Callable[P, AnnotatedDF]:
        logger = logging.getLogger(fn.__module__)
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> AnnotatedDF:
            adf = fn(*args, **kwargs)

            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            params = bound.arguments

            resolved = specs(**params) if callable(specs) else specs

            declared_keys = list(adf.new_cols.keys())
            spec_by_key = _normalize_specs_for_keys(
                resolved,
                declared_keys=declared_keys,
                context=f"{fn.__module__}.{fn.__name__}",
            )

            ctx = get_ctx()

            for decl in adf.new_cols.values():
                if decl.column is None:
                    continue

                spec = spec_by_key.get(decl.key)
                if spec is None:
                    continue

                if ctx is not None:
                    ctx.add_created(key=decl.key, col=decl.column, spec=spec)

                explain_created(logger=logger, col=decl.column, spec=spec)

            return adf

        return wrapper

    return decorator
