### `CONTRIBUTING.md`

```md
# Contributing to QLIR

Thanks for helping build QLIR! This guide covers the dev workflow, code style, tests, and release steps.

---

## Dev Setup

```bash
git clone https://github.com/Trones21/qlir.git
cd qlir
make dev
````

This will:

* create a local `venv/`
* install QLIR in editable mode
* install dev tools (pytest, ruff)

> Prefer **non-captive** venv usage (no `source venv/bin/activate`). The Makefile calls `venv/bin/python` directly for reproducibility.

---

## Useful Make Targets

```bash
make run ARGS="-h"                            # CLI help via module mode
make run ARGS="fetch --symbol SOL-PERP --out data/sol.parquet"
make cli ARGS="--help"                        # after enabling [project.scripts]
make test                                     # run pytest (see makefile for running a single test file or func)
make lint                                     # run ruff check
make format                                   # ruff format + autofix
make build                                    # build wheel/sdist and check
make clean                                    # remove caches and build artifacts
```

---

## Code Layout

```
src/qlir/
  cli.py          # CLI entry point (argparse)
  data/           # data sources (e.g., drift.py) and CSV loaders
  io/             # generic I/O (reader.py, writer.py)
  features/       # (future) feature engineering
  signals/        # (future) signal generation
tests/            # pytest suite
```

* **Imports**: use absolute imports (`from qlir.io import write`) to avoid relative-import pitfalls.
* **I/O**: keep naming generic (no “candles” in I/O); domain-specific names belong in fetchers/sources.
* **Argparse**: group commands under subparsers (`csv`, `fetch`, …) for easy extension.

---

## Style & Tooling

* **Python**: 3.10+
* **Ruff**: configured in `pyproject.toml` (`line-length = 100`, etc.)
* **Tests**: `pytest -q --maxfail=1` (see `tool.pytest.ini_options`)

Run all checks locally:

```bash
make check   # == lint + test
```

---

## Adding a Subcommand

1. Edit `src/qlir/cli.py`:

   * Add a new subparser:

     ```python
     p_feat = sub.add_parser("features", help="Build features")
     p_feat.add_argument("--in", dest="in_path")
     p_feat.add_argument("--out", dest="out_path")
     ```
   * Implement the handler in `main()`:

     ```python
     elif args.cmd == "features":
         df = read(args.in_path)
         feats = build_features(df)       # your function
         write(feats, args.out_path)
     ```
2. Add tests under `tests/`.
3. `make check`.

---

## Versioning & Release (optional)

When you’re ready to share binaries (internal or public):

```bash
make build
# dist/qlir-<version>.tar.gz and .whl

# Upload to your private index (example)
twine upload --repository-url https://your.private.repo/ dist/*
```

**Read-only installs from GitHub** (no index) are also supported:

```
pip install "git+https://github.com/you/qlir.git@v0.1.0"
```

---

## Parquet Dependency

If Parquet is core to your workflows, ensure `pyarrow` is in base deps.
Otherwise, document it as an optional dependency users must install.

---

## Communication

* Open issues/PRs with a concise description and reproduction steps.
* Prefer small, focused PRs (I/O changes, CLI subcommand, feature block, etc.).

Thanks again for contributing!

```
