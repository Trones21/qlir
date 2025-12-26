# afterdata

🚀 **QLIR Quickstart Scaffold**

This is a starter layout for a project that will use the QLIR library.

## ⚙️ Setup

0. Clone qlir if using local dep (the ironic thing is that to use quickstart.py you already cloned, so you just need the path that you cloned to)
1. Update the QLIR dependency in `pyproject.toml` before running install.

```toml
  # Local path dependency (edit as needed)
  # "qlir @ file:///<absolute_path_to_cloned_qlir>/qlir",

  # Or use the Git version:
  # "qlir @ git+https://github.com/Trones21/qlir.git@main",
````

`See the py.project.toml for instructions on how to use an editable version of qlir, this requires some python/poetry knowledge and is not recommended for beginners`

3. Install dependencies and run:

```bash
poetry install

# Just to verify that the install worked 
poetry run main
# or
poetry run python -m afterdata.main
```

4. Get Data and store it to the qlir canonical file location
```bash
poetry run fetch_initial_data
```

5. Edit main.py 


6. (At a later date) Update the dataset on disk 
```bash
poetry run fetch_and_append_new_data
```


## 🧩 Project Structure

```text
afterdata/
  README.md
  pyproject.toml
  etl/
    <source>_etl.py   # functions for managing data layers (fetch raw, clean to df, etc.)
  src/afterdata/
    __init__.py
    main.py
  tests/
  outputs/
  data/
```

* `src/afterdata/main.py` — entry point for your analysis
* `data/` — optional local inputs
* `outputs/` — generated tables, plots, and artifacts
* `tests/` — smoke tests, regression tests, etc.

## 📦 QLIR Data Storage Philosophy

QLIR is designed around a **canonical, disk-first data model**.

### 1. Canonical on-disk layout

By default, QLIR expects candle data under a root like:

```text
~/qlir_data/<datasource>/<instrument>_<resolution>.parquet
```

You can override the root directory, but the structure remains the same.

QLIR standardizes:

* instrument identifiers
* resolution strings
* filename conventions
* metadata schemas

This provides a stable **data identity layer**.

### 2. Disk → network workflow

Every pipeline should obtain candles via a loader such as:

```python
get_candles(instrument, resolution, datasource)
```

Conceptually, QLIR will:

1. Compute the canonical path
2. If the file exists on disk → load it immediately
3. If not → fetch from the network, normalize, and return it
4. Optionally persist the result via the I/O layer

This gives you reproducibility, speed, and deterministic behavior.

### 3. Store canonical 1-minute candles

The intended pattern is:

* store **1m candles** per instrument + datasource on disk
* resample to higher timeframes (5m, 15m, 1h, etc.) in-memory
* only cache higher-res datasets if runtime actually becomes a bottleneck

This avoids an explosion of redundant files while keeping resampling flexible.

### 4. Updating datasets on disk

Use the source specific etl files to get and clean data

Once the canonical data exists on disk, your studies can repeatedly:

* load from disk
* resample
* enrich
* run edge / strategy logic

without re-fetching full history each run.

## 📊 Data & Outputs

When your workflow produces files under `outputs/`, consider including **metadata** about the dataset or parameters that generated them.

Good practice:

* Add an `outputs/README.md` describing the data source and run context.
* If the underlying dataset is too large or private to check in, add a checksum or manifest.
* For large binary artifacts, consider using Git LFS and track only pointers.

If someone sees an output in your repo, they should be able to tell *what data and settings produced it*.

## 💬 Next Steps

* Use this scaffold as a starting point for a single study.
* Gradually extract reusable pieces into shared modules.
* Explore QLIR examples (if present) for patterns and conventions.
