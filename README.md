1. clone qlir, 
2. run ./quickstart/qlir_quickstart.py to create a project 
3. open the project folder and follow the instructions in the generated README


## KMM (Key Mental Model)

<One–three short sentences describing how to think about this module.>

## Why KMM works (especially for QLIR)

QLIR has:

* multiple abstraction layers
* similar-looking primitives with *different semantic contracts*
* time, order, contiguity, and state leaking risks

A **KMM answers one question immediately**:

> *“How should I be thinking about this module before I touch it?”*

Not:

* what functions exist
* how they’re implemented

But:

* **what mental frame must be active**

--- 

Some Examples 

## Example: `core/legs/README.md`

```md
## KMM (Key Mental Model)

A leg is a contiguous, ordered run of rows representing a stable state over time.
Legs are created upstream from boolean condition sets and already exist when this
module is used. Everything here assumes order and contiguity.
```

That alone prevents ~50% of misuse.

---

## Example: `df.condition_set/README.md`

```md
## KMM (Key Mental Model)

Condition sets answer “is this true on this row?” and nothing else.
They are row-local, unordered, and perform no temporal or grouping logic.
They describe *when* something is true, not *for how long*.
```

Notice how this *pairs* with the legs KMM.

---

## Example: `core.ops/README.md`

```md
## KMM (Key Mental Model)

Ops are pure transformations on aligned Series or arrays.
They do not know about time, order, grouping, or finance semantics.
If shuffling rows changes the result, it does not belong here.
```

---

## Example: `core.comparators/README.md`

```md
## KMM (Key Mental Model)

Comparators are row-local boolean predicates.
Each output value depends only on values from the same row.
They never inspect neighboring rows or group structure.
```

---

## Example: `column_bundles/README.md`

```md
## KMM (Key Mental Model)

Column bundles are semantic recipes that combine core primitives
into domain-meaningful artifacts. They are opinionated, named,
and intended to be consumed directly by studies.
```

---

## Hard rule (worth writing down)

> If you cannot write a KMM for a module, the module’s responsibility is unclear.

That’s not a documentation problem — it’s a design problem.
