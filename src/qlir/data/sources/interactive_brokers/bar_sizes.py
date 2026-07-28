"""
IBKR bar-size mapping.

The shared slice machinery keys slices by a canonical `interval` token and relies on
`interval_to_ms` (which supports only `<int>s` / `<int>m`). IBKR, however, names bar
sizes like "1 min" / "1 hour" / "1 day". This module is the single translation point:

    canonical token ("1m", "60m", ...) <-> IBKR barSizeSetting ("1 min", "1 hour", ...)

Phase 1 supports intraday sizes that map cleanly onto seconds/minutes (so the canonical
token stays `interval_to_ms`-parseable). Daily/weekly/monthly bars are intentionally
excluded for now (they don't have a fixed ms span and RTH complicates completeness).
"""

from __future__ import annotations

# Canonical token -> IBKR barSizeSetting. Canonical tokens are all <int>s / <int>m
# so they remain parseable by the shared interval machinery.
CANONICAL_TO_IBKR: dict[str, str] = {
    "1s": "1 secs",
    "5s": "5 secs",
    "10s": "10 secs",
    "15s": "15 secs",
    "30s": "30 secs",
    "1m": "1 min",
    "2m": "2 mins",
    "3m": "3 mins",
    "5m": "5 mins",
    "10m": "10 mins",
    "15m": "15 mins",
    "20m": "20 mins",
    "30m": "30 mins",
    "60m": "1 hour",
    "120m": "2 hours",
    "180m": "3 hours",
    "240m": "4 hours",
    "480m": "8 hours",
}

# Convenience aliases accepted on the CLI, normalized to the canonical token.
ALIASES: dict[str, str] = {
    "1h": "60m",
    "2h": "120m",
    "3h": "180m",
    "4h": "240m",
    "8h": "480m",
}


def normalize_interval(token: str) -> str:
    """Normalize a user token to a supported canonical interval token."""
    t = token.strip().lower()
    t = ALIASES.get(t, t)
    if t not in CANONICAL_TO_IBKR:
        raise ValueError(
            f"Unsupported IBKR bar size {token!r}. "
            f"Supported: {sorted(CANONICAL_TO_IBKR)} (aliases: {sorted(ALIASES)}). "
            "Daily/weekly/monthly bars are not supported in Phase 1."
        )
    return t


def ibkr_bar_size(canonical_interval: str) -> str:
    """Map a canonical interval token to the IBKR barSizeSetting string."""
    return CANONICAL_TO_IBKR[normalize_interval(canonical_interval)]


def interval_to_ms(canonical_interval: str) -> int:
    """
    Milliseconds per bar for a canonical interval token.

    Kept local (rather than importing the Binance helper) so this source has no
    cross-source dependency; values match `<int>s`/`<int>m` semantics.
    """
    t = normalize_interval(canonical_interval)
    value = int(t[:-1])
    unit = t[-1]
    if unit == "s":
        return value * 1_000
    if unit == "m":
        return value * 60_000
    raise ValueError(f"Unsupported interval unit in {t!r}")


def supported_tokens() -> list[str]:
    return sorted(CANONICAL_TO_IBKR) + sorted(ALIASES)
