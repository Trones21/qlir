"""
IB Gateway / TWS client wrapper (ib_async).

Unlike Binance (stateless HTTP, a fresh client per call), IBKR uses a persistent
authenticated socket connection. The worker creates ONE client at startup (connect +
qualify contract) and reuses it across every slice fetch — reconnecting per slice would
be slow and would burn pacing budget.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, List

log = logging.getLogger(__name__)

try:  # pragma: no cover - external dependency wiring
    from ib_async import IB, Stock
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "ib_async is required for qlir.data.sources.interactive_brokers.\n"
        "Install it with: poetry add ib_async  (or: pip install ib_async)"
    ) from exc


def _bar_date_to_ms(date_val: Any) -> int:
    """Normalize an ib_async BarData.date to open_time in ms (formatDate=2 => epoch secs)."""
    if isinstance(date_val, datetime):
        dt = date_val if date_val.tzinfo else date_val.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    # epoch seconds as int or str
    return int(float(date_val)) * 1000


class IBKRHistoricalClient:
    def __init__(self, ib: "IB", contract, *, what_to_show: str, use_rth: int):
        self.ib = ib
        self.contract = contract
        self.what_to_show = what_to_show
        self.use_rth = use_rth

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @classmethod
    def connect(
        cls,
        *,
        host: str,
        port: int,
        client_id: int,
        symbol: str,
        sec_type: str,
        exchange: str,
        currency: str,
        primary_exchange: str | None,
        what_to_show: str,
        use_rth: int,
        timeout: float = 30.0,
    ) -> "IBKRHistoricalClient":
        if sec_type != "STK":
            raise ValueError(f"Phase 1 supports sec_type='STK' only; got {sec_type!r}")

        ib = IB()
        log.info("Connecting to IB Gateway/TWS at %s:%s (clientId=%s)", host, port, client_id)
        ib.connect(host, port, clientId=client_id, timeout=timeout)

        contract = Stock(symbol, exchange, currency, primaryExchange=primary_exchange or "")
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            ib.disconnect()
            raise RuntimeError(
                f"Could not qualify contract for {symbol} "
                f"({sec_type}/{exchange}/{currency}). Check the symbol/exchange/entitlements."
            )
        log.info("Qualified contract: %s", qualified[0])
        return cls(ib, qualified[0], what_to_show=what_to_show, use_rth=use_rth)

    def is_connected(self) -> bool:
        try:
            return bool(self.ib.isConnected())
        except Exception:
            return False

    def disconnect(self) -> None:
        try:
            self.ib.disconnect()
        except Exception:  # pragma: no cover - best-effort
            pass

    # ------------------------------------------------------------------
    # Requests
    # ------------------------------------------------------------------
    def req_head_timestamp_ms(self) -> int | None:
        """Earliest available data point for the contract, in ms (or None)."""
        dt = self.ib.reqHeadTimeStamp(
            self.contract,
            whatToShow=self.what_to_show,
            useRTH=bool(self.use_rth),
            formatDate=2,
        )
        if not dt:
            return None
        if isinstance(dt, datetime):
            d = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return int(d.timestamp() * 1000)
        return int(float(dt)) * 1000

    def req_historical_bars(
        self,
        *,
        end_dt: datetime,
        duration_str: str,
        bar_size_setting: str,
    ) -> List[list]:
        """One reqHistoricalData call -> canonical list-of-lists rows (sorted by open_time)."""
        bars = self.ib.reqHistoricalData(
            self.contract,
            endDateTime=end_dt,
            durationStr=duration_str,
            barSizeSetting=bar_size_setting,
            whatToShow=self.what_to_show,
            useRTH=bool(self.use_rth),
            formatDate=2,
            keepUpToDate=False,
        )
        rows: List[list] = []
        for b in bars:
            rows.append(
                [
                    _bar_date_to_ms(b.date),
                    float(b.open),
                    float(b.high),
                    float(b.low),
                    float(b.close),
                    float(b.volume),
                    float(getattr(b, "average", 0.0) or 0.0),
                    int(getattr(b, "barCount", 0) or 0),
                ]
            )
        rows.sort(key=lambda r: r[0])
        return rows

    # ------------------------------------------------------------------
    # Metadata for the persisted response
    # ------------------------------------------------------------------
    def contract_meta(self) -> dict:
        c = self.contract
        return {
            "symbol": getattr(c, "symbol", None),
            "sec_type": getattr(c, "secType", None),
            "exchange": getattr(c, "exchange", None),
            "currency": getattr(c, "currency", None),
            "con_id": getattr(c, "conId", None),
            "what_to_show": self.what_to_show,
            "use_rth": self.use_rth,
        }
