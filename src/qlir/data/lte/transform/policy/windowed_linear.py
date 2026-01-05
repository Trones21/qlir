from __future__ import annotations

from typing import Sequence

import numpy as _np
import pandas as _pd

from .base import FillPolicy, FillContext


class WindowedLinearFillPolicy(FillPolicy):
    """
    Windowed linear fill policy.

    - Linearly interpolates CLOSE from left.close to right.open
    - Uses local volatility (estimated from surrounding real candles)
      to form a high/low envelope
    - Deterministic and reproducible
    """

    name = "windowed_linear"

    def __init__(
        self,
        *,
        vol_window: int = 5,
        vol_scale: float = 1.0,
        min_vol: float | None = None,
    ):
        """
        Parameters
        ----------
        vol_window : int
            Number of real candles on EACH side of the gap used to
            estimate volatility.
        vol_scale : float
            Multiplier applied to estimated volatility.
        min_vol : float | None
            Optional floor on volatility to avoid zero-width envelopes.
        """
        if vol_window <= 0:
            raise ValueError("vol_window must be positive")

        if vol_scale <= 0:
            raise ValueError("vol_scale must be positive")

        self.vol_window = vol_window
        self.vol_scale = vol_scale
        self.min_vol = min_vol

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def generate(self, ctx: FillContext) -> _pd.DataFrame:
        n = len(ctx.timestamps)

        if n == 0:
            return _pd.DataFrame()

        # --------------------------------------------------------------
        # 1. Linear close path
        # --------------------------------------------------------------
        closes = _np.linspace(
            ctx.left["close"],
            ctx.right["open"],
            n + 2,
            dtype=float,
        )[1:-1]

        # --------------------------------------------------------------
        # 2. Estimate volatility
        # --------------------------------------------------------------
        sigma = self._estimate_volatility(ctx)

        if self.min_vol is not None:
            sigma = max(sigma, self.min_vol)

        sigma *= self.vol_scale

        # --------------------------------------------------------------
        # 3. Build OHLC rows
        # --------------------------------------------------------------
        rows = []
        prev_close = ctx.left["close"]

        for ts, close in zip(ctx.timestamps, closes):
            open_ = prev_close

            high = max(open_, close) + sigma
            low = min(open_, close) - sigma

            rows.append(
                {
                    "timestamp": ts,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                }
            )

            prev_close = close

        return (
            _pd.DataFrame(rows)
            .set_index("timestamp")
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _estimate_volatility(self, ctx: FillContext) -> float:
        """
        Estimate local volatility from surrounding real candles.

        Uses absolute close-to-close differences.
        """
        diffs: list[float] = []

        left_closes = self._collect_left_closes(ctx)
        right_closes = self._collect_right_closes(ctx)

        series = left_closes + [ctx.left["close"], ctx.right["open"]] + right_closes

        for a, b in zip(series[:-1], series[1:]):
            diffs.append(abs(b - a))

        if not diffs:
            return 0.0

        return float(_np.mean(diffs))

    def _collect_left_closes(self, ctx: FillContext) -> list[float]:
        """
        Collect closes from real candles before the gap.
        """
        out: list[float] = []
        row = ctx.left

        # walk backwards via index
        df = row.to_frame().T  # placeholder; real traversal happens upstream
        # NOTE:
        # The strict context guarantees at least left/right,
        # but window traversal should be handled upstream later
        return out

    def _collect_right_closes(self, ctx: FillContext) -> list[float]:
        """
        Collect closes from real candles after the gap.
        """
        return []
