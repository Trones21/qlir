from __future__ import annotations

import functools
import inspect
import logging
from typing import Callable, Mapping, ParamSpec, Sequence, Union

from qlir.core.types.new_cols_result import NewColsResult

from .context import get_ctx
from .explain import explain_created
from .row_derivation import ColumnDerivationSpec

ReturnedCols = str | Sequence[str] | Mapping[str, str]

Specs = Union[
    ColumnDerivationSpec,
    Sequence[ColumnDerivationSpec],
    Mapping[str, ColumnDerivationSpec],
]

SpecsOrCallable = Union[
    Specs,
    Callable[..., Specs],
]

def _normalize_returned_cols(cols: ReturnedCols) -> tuple[list[str], dict[str, str] | None]:
    """
    Returns:
      - flat list of column names (in stable order)
      - optional role->col mapping if provided as dict
    """
    if isinstance(cols, str):
        return [cols], None

    if isinstance(cols, Mapping):
        # Preserve insertion order (py3.7+ dict order is stable).
        role_map = dict(cols)
        return list(role_map.values()), role_map

    # Sequence[str]
    return list(cols), None


def _resolve_specs(
    *,
    specs: ColumnDerivationSpec
        | Sequence[ColumnDerivationSpec]  
        | Mapping[str, ColumnDerivationSpec],        
    out_cols: list[str],
    role_map: dict[str, str] | None,
) -> list[tuple[str, ColumnDerivationSpec]]:
    """
    Returns (col, spec) pairs aligned to output columns.
    """
    if isinstance(specs, ColumnDerivationSpec):
        if len(out_cols) != 1:
            raise ValueError(
                f"new_col_func got 1 spec but function returned {len(out_cols)} cols: {out_cols}"
            )
        return [(out_cols[0], specs)]

    if isinstance(specs, Mapping):
        if role_map is None:
            raise ValueError("new_col_func specs is role-mapped but function did not return a dict")
        pairs: list[tuple[str, ColumnDerivationSpec]] = []
        for role, spec in specs.items():
            if role not in role_map:
                raise KeyError(f"new_col_func missing role '{role}' in returned cols {list(role_map)}")
            pairs.append((role_map[role], spec))
        return pairs

    # Sequence[spec]
    specs_list = list(specs)
    if len(specs_list) != len(out_cols):
        raise ValueError(
            f"new_col_func got {len(specs_list)} specs but function returned {len(out_cols)} cols: {out_cols}"
        )
    return list(zip(out_cols, specs_list))

P = ParamSpec("P")

def new_col_func(
    *,
    specs: SpecsOrCallable,
) -> Callable[[Callable[P, NewColsResult]], Callable[P, NewColsResult]]:
    """
    Decorator for functions that create new column(s).

    Contract:
      - wrapped function returns (df, out_cols)
      - out_cols can be:
          * str
          * list/tuple[str]
          * dict[str, str] (role -> column)
      - derivation specs are recorded (if a DerivationContext exists) and logged.
    """
    
    def decorator(fn: Callable[P, NewColsResult]) -> Callable[P, NewColsResult]:
        logger = logging.getLogger(fn.__module__)
        sig = inspect.signature(fn)
        
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            df, out_cols_any = fn(*args, **kwargs)

            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            params = bound.arguments  # dict: param_name -> value

            # 👇 resolve specs dynamically
            resolved_specs = specs(**params) if callable(specs) else specs

            out_cols, role_map = _normalize_returned_cols(out_cols_any)
            pairs = _resolve_specs(specs=resolved_specs, out_cols=out_cols, role_map=role_map)

            ctx = get_ctx()
            for col, spec in pairs:
                # Record always, if context exists
                if ctx is not None:
                    ctx.add_created(col=col, spec=spec)
                # Log always (current behavior; you can gate this later)
                explain_created(logger=logger, col=col, spec=spec)

            return df, out_cols_any

        return wrapper

    return decorator




# def new_col_func(
#     *,
#     specs: ColumnDerivationSpec | 
#         Sequence[ColumnDerivationSpec] | 
#         Mapping[str, ColumnDerivationSpec] |
#         Callable[..., ColumnDerivationSpec]
# ) -> Callable[[Callable[P, NewColsResult]], Callable[P, NewColsResult]]: