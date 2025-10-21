
### `USAGE.md`

````md
# QLIR — Usage Guide

QLIR (**Quant Layered Indicator Runtime**) is a Python library + CLI for building pipelines that transform **Indicators → Features → Signals** on top of Pandas.

---

## Install

### From GitHub (read-only, no push access)
```bash
# HTTPS (public or with a token for private)
pip install "git+https://github.com/Trones21/qlir.git@main"

# SSH (if your GitHub SSH keys are set up)
pip install "git+ssh://git@github.com/Trones21/qlir.git@main"
````

Pin a tag/commit for reproducibility:

```bash
pip install "git+https://github.com/you/qlir.git@v0.1.0"
# or a specific commit:
pip install "git+https://github.com/you/qlir.git@abc1234"
```

> **Parquet support**: ensure a Parquet engine is available (e.g., `pyarrow`).
> If it’s not in base deps, install it yourself: `pip install pyarrow`.

---

## CLI

After install, the `qlir` command is available.

```bash
qlir -h
```

### Subcommands

* `qlir csv <path>` — Load and echo a normalized CSV (first rows).
* `qlir fetch [--symbol SYMBOL] [--res RES] [--limit N] [--out PATH] [--compression CODEC]`

  * Example:

    ```bash
    qlir fetch --symbol SOL-PERP --res 1 --limit 500 --out data/sol_1m.parquet
    qlir fetch --symbol SOL-PERP --limit 1000 --out data/sol_1m.json
    ```

> Output format is inferred by extension: `.csv`, `.parquet`, `.json`.
> For Parquet compression, use `--compression zstd|snappy|gzip|brotli` (default usually `snappy`).

---

## Python API

```python
from qlir.data.drift import fetch_drift_candles
from qlir.io import write, read

# Fetch raw market data (Drift)
df = fetch_drift_candles(symbol="SOL-PERP", resolution="1", limit=500)

# Write to your preferred format (inferred by extension)
write(df, "data/sol_1m.parquet")

# Read it back
df2 = read("data/sol_1m.parquet")
print(df2.tail())
```

---

## Supported I/O Formats

| Format     | Read | Write | Notes                                                  |
| ---------- | ---- | ----- | ------------------------------------------------------ |
| `.csv`     | ✅    | ✅     | Human-readable                                         |
| `.parquet` | ✅    | ✅     | Compressed columnar (needs `pyarrow` or `fastparquet`) |
| `.json`    | ✅    | ✅     | For web/interop; JSONL via `lines=True`                |

Use `qlir.io.read()` / `qlir.io.write()` to auto-dispatch by file extension.

---

## Quick Troubleshooting

* **`ModuleNotFoundError` for `pyarrow`**: install `pyarrow` (`pip install pyarrow`) for Parquet.
* **`qlir: command not found`**: ensure the environment you installed into is the one you’re running from (e.g., virtualenv/conda).
* **Private repo auth**: for HTTPS installs to a private repo, use a GitHub token in the URL or switch to SSH.