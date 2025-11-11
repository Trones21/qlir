### Testing

pytest.ini has marks for specific subsets of tests. Marks can be completely decoupled from directiory structure.

Currently I have:

```text
markers =
    network: test requires network access (HTTP, RPC, real API)
    local: test must not make network calls
    integration: Integration tests - may or may not make network calls 
```

To mark all tests within a file, add:

```python
import pytest
pytestmark = pytest.mark.<marker>
```

To mark a specific function, add decorator(s):

```python
@pytest.mark.<marker>
@pytest.mark.<marker>
def test_add_new_candles_to_dataset_grows_file(tmp_path: Path):
```

See the makefile for task runner commands/aliases (run subset, run file, run func etc.)
