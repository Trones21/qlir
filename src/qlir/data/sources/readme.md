# Synthetic Sources

This module constructs **synthetic price sources** from multiple existing sources.

Synthetic construction creates a *new* price series whose values may differ from
any individual input source. It is not a repair mechanism and does not preserve
exchange-level price authority.

## What this module does

- Combines multiple aligned price sources into a single synthetic source
- May overwrite observed prices by design
- Operates fully in-memory
- Produces a DataFrame that can be consumed like a real exchange feed

Typical use cases:
- Consensus pricing across exchanges
- Disagreement / robustness studies
- Signal construction independent of any single venue

## What this module does NOT do

- It does **not** fill gaps in a single source
- It does **not** repair missing OHLC values
- It does **not** privilege a “primary” source unless the policy explicitly does so

To backfill gaps in one source using fallback sources, use the `gaps` module.

## Relationship to gap filling

Gap filling and synthetic construction are intentionally separate operations:

| Operation | Purpose | Overwrites prices? |
|---------|--------|--------------------|
| Gap fill (`gaps`) | Repair a source | No |
| Synthetic construction | Create a new source | Yes |

## Composition example

A common workflow is to first **repair** a primary source using ordered fallback
sources, and then **construct** a synthetic source from repaired data.

```python
# Step 1: repair Binance using Drift as a fallback
binance_repaired = apply_fill_policy(
    df=binance,
    policy=OrderedSourceFillPolicy(
        sources=[
            ("binance", binance),
            ("drift", drift),
        ],
        ohlc_cols=OHLC,
    ),
)

# Step 2: construct a synthetic source from repaired Binance and Coinbase
synthetic = construct(
    sources={
        "binance": binance_repaired,
        "coinbase": coinbase,
    },
    policy=MedianPolicy(),
    ohlc_cols=OHLC,
    interval_s=60,
)
