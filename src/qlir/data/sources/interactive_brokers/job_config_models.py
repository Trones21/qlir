# ---------------------------------------------------------------------------
# Job configuration models (Interactive Brokers)
# ---------------------------------------------------------------------------

from dataclasses import dataclass


@dataclass(frozen=True)
class HistoricalBarsJobConfig:
    """
    Configuration for a single IBKR historical-bars ingestion job.

    Mirrors the Binance KlinesJobConfig shape (symbol/interval/limit) so the shared
    slice/manifest/claims machinery works unchanged, and adds the extra fields IBKR
    needs that Binance did not:

      - contract identity (sec_type/exchange/currency/primary_exchange), because an
        IBKR instrument is a Contract, not a bare ticker.
      - request semantics (what_to_show/use_rth).
      - the Gateway/TWS socket connection (host/port/client_id).

    `interval` is a canonical token (e.g. "1s", "1m", "5m", "60m") — see bar_sizes.py.
    `limit` is bars-per-slice (the IBKR analog of Binance's per-request candle limit),
    which keeps the on-disk `limit=<n>` path segment identical to Binance.
    """

    symbol: str                       # e.g. "AAPL"
    interval: str = "1m"              # canonical token; mapped to an IBKR barSizeSetting
    limit: int = 1000                # bars per slice (window size)

    # ---- contract identity (STK is the Phase-1 target) ----
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    primary_exchange: str | None = None   # optional disambiguation, e.g. "NASDAQ"

    # ---- request semantics ----
    what_to_show: str = "TRADES"
    use_rth: int = 0                  # 0 = include pre/post-market, 1 = regular hours only

    # ---- IB Gateway / TWS connection ----
    host: str = "127.0.0.1"
    port: int = 4002                  # 4002 paper gateway / 4001 live gateway (7497/7496 for TWS)
    client_id: int = 1
