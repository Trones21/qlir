# KMM — Key Mental Model

> **What this file is — and isn't.**
> A **KMM** is a one-to-three sentence note at the top of a module that answers a single
> question: *"How should I be thinking about this module before I touch it?"* — not what
> functions exist or how they're implemented, but **what mental frame must be active.**
>
> This file is **not an exhaustive list of QLIR's KMMs.** Real KMMs are meant to live
> *throughout* the codebase (mostly the TA-library side — `core/`, `df/`, `column_bundles/`,
> etc.), each next to the module it describes. The examples below exist only to give you a
> **feel** for what a good KMM looks like and why they matter. Treat them as a pattern to
> copy, not a registry to maintain.

---

## Why KMM works (especially for QLIR)

QLIR has:

* multiple abstraction layers
* similar-looking primitives with *different semantic contracts*
* time, order, contiguity, and state leaking risks

A **KMM answers one question immediately**:

> *"How should I be thinking about this module before I touch it?"*

Not:

* what functions exist
* how they're implemented

But:

* **what mental frame must be active**

---

## Examples (illustrative, not exhaustive)

### `core/legs/README.md`

```md
## KMM (Key Mental Model)

A leg is a contiguous, ordered run of rows representing a stable state over time.
Legs are created upstream from boolean condition sets and already exist when this
module is used. Everything here assumes order and contiguity.
```

That alone prevents ~50% of misuse.

### `df.condition_set/README.md`

```md
## KMM (Key Mental Model)

Condition sets answer "is this true on this row?" and nothing else.
They are row-local, unordered, and perform no temporal or grouping logic.
They describe *when* something is true, not *for how long*.
```

Notice how this *pairs* with the legs KMM.

### `core.ops/README.md`

```md
## KMM (Key Mental Model)

Ops are pure transformations on aligned Series or arrays.
They do not know about time, order, grouping, or finance semantics.
If shuffling rows changes the result, it does not belong here.
```

### `core.comparators/README.md`

```md
## KMM (Key Mental Model)

Comparators are row-local boolean predicates.
Each output value depends only on values from the same row.
They never inspect neighboring rows or group structure.
```

### `column_bundles/README.md`

```md
## KMM (Key Mental Model)

Column bundles are semantic recipes that combine core primitives
into domain-meaningful artifacts. They are opinionated, named,
and intended to be consumed directly by studies.
```

---

## Hard rule (worth writing down)

> If you cannot write a KMM for a module, the module's responsibility is unclear.

That's not a documentation problem — it's a design problem.
