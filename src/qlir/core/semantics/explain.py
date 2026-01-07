from __future__ import annotations

import logging
from typing import Iterable, Optional

from .context import DerivationContext
from .row_derivation import ColumnDerivationSpec


def explain_created(
    *,
    logger: logging.Logger,
    col: str,
    spec: ColumnDerivationSpec,
) -> None:
    logger.info("%s", spec.to_human(write_col=col))

    # Optional guardrail: self-inclusion is a common foot-gun.
    if spec.self_inclusive and spec.read_rows[1] == 0:
        logger.debug(
            "⚠ %s is self-inclusive at row i (rows_used ends at i). "
            "If used for row-level decisions, consider shifting / row-exclusion.",
            spec.op.upper(),
        )


def explain_dropped(
    *,
    logger: logging.Logger,
    col: str,
    reason: str | None,
) -> None:
    if reason:
        logger.info("DROP | col=%s | reason=%s", col, reason)
    else:
        logger.info("DROP | col=%s", col)


def explain_context(
    *,
    logger: logging.Logger,
    ctx: DerivationContext,
    include_intermediate: bool = True,
    include_drops: bool = True,
) -> None:
    for col, spec in ctx.specs:
        if (not include_intermediate) and spec.scope == "intermediate":
            continue
        explain_created(logger=logger, col=col, spec=spec)

    if include_drops:
        for e in ctx.lifecycle:
            if e.event == "dropped":
                explain_dropped(logger=logger, col=e.col, reason=e.reason)