Absolutely. Below is a **full, clean revision** of `module_arch.md`, rewritten to reflect the *final, clarified mental model* you just articulated:

* users import **modules**, not functions
* some modules already exist as files
* some modules must be *constructed* via `_api.py`
* `_api.py` exists purely to shape UX
* matching file/function names are fine when the file is **not** the module boundary
* import syntax is intentionally consistent

This version removes earlier ambiguity and centers everything on **UX-first module design**.

---

````md
# Module Architecture & Import UX (QLIR)

> Last updated: 2026-01-07  
> Audience: QLIR maintainers (primarily future me)

This document defines how **module boundaries** are designed across QLIR
and when an internal `_api.py` façade is required.

The goal is not to reflect Python’s technical module system,
but to provide a **clean, predictable import UX** that we actively guide users toward.

---

## Core philosophy (TL;DR)

> **Users import modules, not functions.**  
> Functions are endpoints *exposed by modules*, not modules themselves.

Everything in this document follows from that principle.

---

## What is a “module boundary”?

A **module boundary** is the unit of code that users are meant to:

- import
- reason about
- see in autocomplete
- treat as stable API surface

That boundary is a **UX decision**, not a Python technicality.

In QLIR, a module boundary may correspond to:
- a directory
- a single file
- or a directory whose surface is constructed via `_api.py`

What matters is **what users import**, not how files are arranged internally.

---

## Preferred user import style (library-wide)

We intentionally guide users toward:

```python
from qlir import indicators
from qlir.core.ops import temporal
````

and *away* from:

```python
from qlir.indicators.rsi import rsi
from qlir.core.ops.temporal import with_diff
```

This keeps:

* imports readable
* namespaces discoverable
* APIs stable under refactors

---

## Case A: File already is the desired module boundary (no `_api.py`)

### When this applies

* Users think in *families* of related operations
* Grouping is part of the mental model
* The file name is the module users should import

### Example: `ops.temporal`

#### Filesystem

```text
qlir/core/ops/
├── temporal.py
├── non_temporal.py
```

#### Usage (guided UX)

```python
from qlir.core.ops import temporal

temporal.with_diff(...)
temporal.with_lag(...)
temporal.with_rolling(...)
```

### Interpretation

* `temporal` is the **module**
* functions live *inside* the module
* users import the module, not the functions
* multiple public functions per file are expected and correct

### Result

No `_api.py` is needed because the filesystem already matches the intended UX.

---

## Case B: Desired module spans multiple implementation files (`_api.py` required)

### When this applies

* Users should import a **single conceptual module**
* Implementation is split across multiple files
* Users should *not* think about submodules
* We want a flat, ergonomic surface

This is the **API façade pattern**.

---

### Example: `indicators`

#### Filesystem

```text
qlir/indicators/
├── __init__.py
├── _api.py
├── rsi.py
├── atrp.py
├── sma.py
```

#### `_api.py`

```python
from .rsi import rsi
from .atrp import atrp
from .sma import sma

__all__ = ["rsi", "atrp", "sma"]
```

#### `__init__.py`

```python
from ._api import *
```

#### Usage (guided UX)

```python
from qlir import indicators

indicators.rsi(...)
indicators.atrp(...)
indicators.sma(...)
```

---

### Interpretation

* `indicators` is the **module boundary**
* `rsi.py`, `atrp.py`, etc. are *implementation units*
* although `rsi.py` is technically a Python module,
  it is **not** a conceptual module in the API
* users should never import implementation files directly

---

## Important clarification: matching file/function names are safe

```text
rsi.py → def rsi()
```

This is **not a problem** because:

* the file is not the module boundary
* the API layer controls exposure
* users only interact with `indicators.rsi`
* naming conflicts only matter *at the boundary*

This is no different from an HTTP API:

* internal handlers can be named freely
* only exposed endpoints define the contract

---

## Why `_api.py` exists (and why it is private)

`_api.py` exists solely to:

* construct the module UX we want users to import
* flatten multiple implementations into one namespace
* prevent leakage of internal structure
* keep autocomplete focused on domain concepts

It is prefixed with `_` because:

* it is infrastructure
* it is not domain logic
* users should not reason about it

If autocomplete suggests `indicators._api`, that is acceptable;
docs and examples should never mention it.

---

## What NOT to do

* Do not expose implementation modules accidentally
* Do not mix boundary styles within a single domain
* Do not add `_api.py` where the file already *is* the module
* Do not guide users to import functions directly

---

## Design rule of thumb (the keeper)

> **Design the module you want users to import.
> Use `_api.py` only when the filesystem doesn’t already give you that module.**

Everything else is implementation detail.

---

## Summary

* QLIR is module-first, not function-first
* Users import modules; functions are accessed through modules
* Some modules map directly to files
* Some modules are assembled via `_api.py`
* `_api.py` exists purely for UX ergonomics
* Import syntax is intentionally consistent

If you are unsure how to structure a new domain:
**start from the import you want users to write, then work backwards.**
