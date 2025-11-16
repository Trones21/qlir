
## 1. Split QLIR conceptually into **core** and **research**

Right now everything is “QLIR.” In your head it’s actually:

* **QLIR-core** → identity + catalog
* **QLIR-research** → indicators, stats, pipelines, disk IO, etc.

You don’t have to make two repos immediately; start with two **subpackages**:

```text
qlir/
  core/
    __init__.py
    instruments.py       # CanonicalInstrument, InstrumentMeta
    timefreq.py          # TimeFreq
    datasource.py        # DataSourceKind, DataTier, DataSource, DataSourceSpec
    symbol_map_base.py   # BaseSymbolMap
    universes.py         # MAJORS, ALTCOINS, etc.
  research/
    indicators/
    pipelines/
    io/
    stats/
```

Then:

* **SPL depends only on `qlir.core`**
* **QLIR’s research code depends on `qlir.core` too**
* **No one in SPL imports `qlir.research.*`**

That gives you:

* Shared naming + identity
* Minimal coupling
* Clear boundaries

---

## 2. What SPL is allowed to import

From SPL’s point of view, the ONLY things it should touch are:

```python
from qlir.core.instruments import CanonicalInstrument
from qlir.core.timefreq import TimeFreq
from qlir.core.datasource import DataSource, DataTier
from qlir.core.symbol_map_base import BaseSymbolMap
from qlir.core.universes import MAJORS, ALTCOINS, ...
```

SPL then uses those to:

* validate config (`execution_source`, `instruments`, `universe`)
* pick an execution venue (`supports_execution`)
* map canonical → venue ids for its adapters
* log consistent instrument names
* maybe choose risk profiles per canonical instrument

But SPL itself (the library) **does not** do:

```python
from qlir.research.indicators import atr
from qlir.research.io import candles_from_disk_or_network
```

(SPL strategy execution projects will **MOST DEFINITELY** use this)

---

## 3. Think of `qlir.core` as a tiny “contract” library

Mentally:

* `qlir.core` = **contract** between all parts of the system
* `qlir.research` = one consumer of that contract
* `spl` = another consumer of that contract

If you ever want to go harder on decoupling, you can literally extract it:

```text
tradesys_core/
  instruments.py
  timefreq.py
  datasource.py
  symbol_map_base.py
  universes.py

qlir/        -> depends on tradesys_core
spl/         -> depends on tradesys_core
```

But you don’t need to start there. Just keep `qlir.core` clean and self-contained, and you can extract it later without pain.

---

## 4. Where `supports_execution` fits in this picture

This helps lock the mental model:

* `supports_execution` lives on **`DataSourceSpec` in `qlir.core.datasource`**
* SPL imports that and does:

  ```python
  src = DataSource.DRIFT  # from qlir.core
  if not src.supports_execution:
      raise ValueError("Invalid execution source")
  ```

QLIR itself never uses `supports_execution` for anything.
It just **declares** it in the catalog.

SPL is the one **enforcing** it.

So the coupling is:

* SPL → `qlir.core.datasource` (tiny, stable)
* Not SPL → `qlir.research` (big, unstable)

That’s good coupling.

---

## 5. Concrete example of “minimal coupling” usage

SPL config:

```toml
[spl.engine]
execution_source = "drift"
instruments = ["SOL_PERP", "BTC_PERP"]
```

SPL bootstrap:

```python
from qlir.core.datasource import DataSource
from qlir.core.instruments import CanonicalInstrument

src = DataSource[config.execution_source.upper()]

if not src.supports_execution:
    raise ValueError(f"{src.name} cannot be used for execution")

instruments = [
    CanonicalInstrument[name]
    for name in config.instruments
]
```

That’s it. SPL doesn’t know about QLIR pipelines, candles, indicators — just the **vocabulary**.

---

## 6. Summary: how to get what you want

You’re aiming for:

> “SPL can leverage the naming and world model QLIR pins down, **without** SPL being coupled to QLIR’s internals.”

You get that by:

1. Making a small **`qlir.core`** subpackage that only handles:

   * instruments
   * timefreq
   * datasource + tiers + supports_execution
   * symbol map base
   * universes
2. Ensuring SPL **only imports from `qlir.core`**
   (never `qlir.research`).
3. Treating `qlir.core` as a tiny shared contract that could later be its own package (`tradesys_core`).

That way SPL *does* leverage the naming,
but the dependency surface area stays small and clean.
