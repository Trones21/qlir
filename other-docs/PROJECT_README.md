# 🧭 QLIR Project Architecture Overview

## 📁 Directory Layout

```
qlir/
├── pyproject.toml          # Project metadata, deps, CLI entry point
├── Makefile                # Dev automation (setup, run, test, lint)
├── src/
│   └── qlir/
│       ├── __init__.py
│       ├── cli.py          # CLI entry point
│       └── data/
│           ├── __init__.py
│           ├── csv.py      # Local CSV loader
│           └── drift.py    # Drift candles fetcher
├── tests/
│   └── ...                 # pytest-based tests
└── venv/                   # Local virtualenv (ignored by git)
```

---

## ⚙️ Layer 1: Project Configuration (`pyproject.toml`)

### Key role

Defines **how QLIR is installed**, its dependencies, and its CLI entry point.

```toml
[project]
name = "qlir"
version = "0.1.0"
dependencies = ["pandas", "numpy", "requests"]

[project.scripts]
qlir = "qlir.cli:main"   # CLI entry point

[tool.setuptools.packages.find]
where = ["src"]
```

✅ This means:

* Code lives under `src/qlir`
* When installed, a command `qlir` is created that runs `qlir.cli:main()`

---

## 🧩 Layer 2: CLI Entry Point (`src/qlir/cli.py`)

### Key role

Implements subcommands via `argparse`, acts as the **public interface** of the package.

```python
import argparse, sys
from qlir.data.csv import load_ohlcv_from_csv
from qlir.data.drift import fetch_drift_candles

def main():
    p = argparse.ArgumentParser(prog="qlir")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_csv = sub.add_parser("csv", help="Load and echo normalized CSV")
    p_csv.add_argument("path")

    p_fetch = sub.add_parser("fetch", help="Fetch Drift candles")
    p_fetch.add_argument("--symbol", default="SOL-PERP")
    p_fetch.add_argument("--res", default="1")
    p_fetch.add_argument("--limit", type=int)

    args = p.parse_args()
    if args.cmd == "csv":
        df = load_ohlcv_from_csv(args.path)
        print(df.head(20).to_string(index=False))
    elif args.cmd == "fetch":
        df = fetch_drift_candles(symbol=args.symbol, resolution=args.res, limit=args.limit)
        print(df.tail().to_string(index=False))
    else:
        p.print_help(); sys.exit(2)
```

✅ Usage:

```bash
python -m qlir.cli csv data/example.csv
python -m qlir.cli fetch --symbol SOL-PERP --limit 50
```

✅ After installation:

```bash
qlir csv data/example.csv
qlir fetch --symbol SOL-PERP --limit 50
```

---

## 🧰 Layer 3: Developer Automation (`Makefile`)

### Key role

Provides shortcuts for environment setup and maintenance.

| Target                         | Purpose                         | Equivalent Command                                            |
| ------------------------------ | ------------------------------- | ------------------------------------------------------------- |
| `make dev`                     | Create venv + install deps      | `python -m venv venv && venv/bin/pip install -e ".[dev]"`     |
| `make run ARGS="csv data.csv"` | Run the CLI                     | `venv/bin/python -m qlir.cli csv data.csv`                    |
| `make cli ARGS="--help"`       | Run installed CLI entry point   | `venv/bin/qlir --help`                                        |
| `make test`                    | Run tests                       | `venv/bin/pytest`                                             |
| `make lint`                    | Run linter                      | `venv/bin/ruff check src tests`                               |
| `make clean`                   | Delete caches + build artifacts | `rm -rf venv build dist *.egg-info .ruff_cache .pytest_cache` |

---

## 🧠 Layer 4: Execution Flow

### 1. Development setup

```bash
make dev
```

Creates virtualenv, installs dependencies (editable mode), and makes the `qlir` CLI available.

### 2. Running the CLI

```bash
make run ARGS="-h"
# or directly:
python -m qlir.cli -h
# or (after install)
qlir -h
```

### 3. Testing and Linting

```bash
make test
make lint
```

---

## 🧩 Layer 5: Import Model

* **Absolute imports** keep everything consistent:

  ```python
  from qlir.data.csv import load_ohlcv_from_csv
  ```
* **Never** use relative imports like:

  ```python
  from .data.csv import load_ohlcv_from_csv
  ```

  — because they fail if you run the file directly.

---

## 🧭 Layer 6: Run Modes

| Mode                   | Command               | Context                                       |
| ---------------------- | --------------------- | --------------------------------------------- |
| **Module Mode**        | `python -m qlir.cli`  | Direct execution (no install needed)          |
| **Installed CLI Mode** | `qlir`                | After `[project.scripts]` entry point install |
| **Make Shortcut**      | `make run ARGS="..."` | Task automation wrapper                       |

---

## 🧩 Summary Diagram

```
      ┌───────────────────────────────────┐
      │ make run ARGS="csv data.csv"      │
      └──────────────┬────────────────────┘
                     │ expands to
                     ▼
      venv/bin/python -m qlir.cli csv data.csv
                     │
                     ▼
        ┌──────────────────────────┐
        │ qlir/cli.py → main()     │
        │  ├── qlir.data.csv       │
        │  └── qlir.data.drift     │
        └──────────────────────────┘
                     │
                     ▼
               pandas DataFrame
```