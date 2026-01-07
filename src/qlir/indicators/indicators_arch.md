# Indicators Architecture (QLIR)

> Last updated: 2026-01-07  
> Audience: QLIR maintainers (primarily future me)

This document describes the **architecture, boundaries, and naming constraints**
for the `qlir.indicators` package.

It exists to prevent:
- accidental API leakage
- editor / language-server confusion
- re-debugging naming issues months later
- erosion of the public indicator surface

---

## High-level goal

Expose indicators as **first-class domain primitives** with a flat, readable API:

```python
from qlir import indicators

df, cols = indicators.rsi(df, window=14)
df, cols = indicators.atrp(df, window=20)
```

Users should not need to understand:

* registries
* dispatch
* internal modules
* implementation layout

---

## Core principle: API boundary is authoritative

> **Only what is explicitly exposed by the API layer is public.**

This mirrors any external API:

* endpoints are defined by the API
* internal implementation details are irrelevant
* names only matter at the boundary

As long as the boundary is respected, internal naming choices are safe.

---

## Directory structure

```text
qlir/
├── indicators/
│   ├── __init__.py        # public façade (re-export only)
│   ├── _api.py            # authoritative public surface
│   ├── rsi.py             # implementation
│   ├── atrp.py
│   ├── sma.py
│   └── ...
```

### Responsibilities

| File                   | Responsibility                            |
| ---------------------- | ----------------------------------------- |
| `*_py` (e.g. `rsi.py`) | Indicator implementation                  |
| `_api.py`              | Public API definition + internal registry |
| `__init__.py`          | Namespace façade                          |

---

## API façade pattern

### `_api.py`

`_api.py` is the **single source of truth** for:

* which indicators exist
* which names are public
* what users can call

Example:

```python
from .rsi import rsi
from .atrp import atrp
from .sma import sma

__all__ = ["rsi", "atrp", "sma"]

_INDICATORS = {
    "rsi": rsi,
    "atrp": atrp,
    "sma": sma,
}

def _apply_indicator(df, name: str, **kwargs):
    fn = _INDICATORS.get(name.lower())
    if fn is None:
        raise ValueError(f"Unknown indicator: {name}")
    return fn(df, **kwargs)
```

Notes:

* Registry is **explicit**, not reflective
* Dynamic dispatch exists but is **internal**
* Public API remains function-first

---

### `__init__.py`

```python
from ._api import *
```

`__init__.py` must:

* contain no logic
* contain no hand-maintained imports
* expose *only* what `_api.py` declares public

---

## Naming guidance (important nuance)

### Allowed and safe

```text
rsi.py   → def rsi()
atrp.py  → def atrp()
```

This is **safe** because:

* modules are not exposed directly
* users interact only through `indicators.<name>`
* the API layer defines the namespace

Language-server issues only arise when a module with the same name
is exposed as a public attribute.

---

### Risky (only if API boundary is violated)

Problems occur if:

* implementation modules are imported directly
* modules are re-exported unintentionally
* users bypass the API façade

Example of what **not** to encourage:

```python
from qlir.indicators import rsi   # bypasses API boundary
import qlir.indicators.rsi
```

---

## Hard rule

> **Users must go through the API façade.**

All examples, docs, notebooks, and tests should use:

```python
from qlir import indicators
```

Never reference:

* `_api`
* implementation modules
* internal registries

---

## Why `api` is private

`_api.py` is intentionally private because:

* it is infrastructure, not domain logic
* users should not reason about dispatch
* autocomplete should surface indicators, not plumbing

Underscore here is **semantically correct**.

---

## Non-goals

* No reflection-based auto-discovery
* No magic registration
* No dynamic imports
* No public “apply_indicator” function

Indicators are primitives, not plugins.

---

## Summary (TL;DR)

* API boundary defines what is public
* Implementation modules may share names with functions safely
* All user access goes through `indicators`
* `_api.py` is the single source of truth
* `__init__.py` is a thin façade
* Internal dispatch is invisible

If something feels ambiguous, noisy, or “frameworky” to the user,
it probably does not belong in the public API.

Future-me:
If you are tempted to expose an implementation detail, stop and reread this.
