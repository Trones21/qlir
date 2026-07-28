from __future__ import annotations

# Meta-contract fields we expect fetch_and_persist_slice to return (see worker._update_entry).
# Mirrors the Binance klines model.py, minus HTTP/URL-specific fields (IBKR has neither).
REQUIRED_FIELDS = [
    "slice_id",
    "relative_path",
    "slice_status",
    "slice_status_reason",
    "n_items",
    "first_ts",
    "last_ts",
    "requested_at",
    "completed_at",
]

# Canonical column order of a persisted IBKR bar row (list-of-lists, mirrors the
# Binance kline row shape so downstream inspection/agg treat r[0] as open_time ms).
BAR_COLUMNS = [
    "open_time",   # ms since epoch (bar open)
    "open",
    "high",
    "low",
    "close",
    "volume",      # NOTE: IBKR stock volume is in lots (x100 shares) — a data-semantics
                   # concern for the (Phase 2) agg schema, not for ingestion.
    "average",
    "bar_count",
]
