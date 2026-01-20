Nice — that’s a good instinct to keep the **README short and pragmatic**, while still hinting at the longer-term direction.

Below is a **tight rewrite** that:

* keeps everything you already have,
* adds the *why* in just a few bullets,
* clearly labels the rest as **future goals**, not current complexity.

You can drop this straight into your README.

---

### Testing

QLIR uses **pytest** with explicit markers to control *how* and *where* tests run.
Markers are intentionally **decoupled from directory structure**.

`pytest.ini` defines the following markers:

```text
markers =
    network: test requires network access (HTTP, RPC, real API)
    local: test must not make network calls
    integration: integration tests (may or may not use network)
    datasources: tests related to data source integrations
    datasource.<source>: tests specific to a given data source
    datasource.<source>.<endpoint>: tests for a specific endpoint
```

> Note: not all endpoints necessarily have dedicated markers — always check the test itself.

---

#### Marking tests

To mark **all tests in a file**:

```python
import pytest

pytestmark = pytest.mark.<marker>
```

To mark a **specific test function**:

```python
@pytest.mark.<marker>
@pytest.mark.<marker>
def test_add_new_candles_to_dataset_grows_file(tmp_path: Path):
    ...
```

See the **Makefile** for task runner commands and shortcuts
(e.g. run by marker, run a file, run a single test).

---

#### Design goals (not fully implemented yet)

The current marker system is intentionally simple, but it’s designed to scale toward the following goals:

* **Decouple test directory structure from library structure**
  Tests should describe *what they protect*, not *where code lives*.

* **Mark tests by fully-qualified library targets (FQN)**
  e.g. `package.module.function` (same format as `pyproject` entry points),
  so external tooling can reason about:

  * which APIs are protected
  * which are untested
  * intentional vs incidental coverage

This keeps tests stable as files move, analyses evolve, and code is refactored.

