#!/usr/bin/env python3
from pathlib import Path
from textwrap import dedent
import sys
import re
import keyword

def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def main():
    if len(sys.argv) < 2:
        print("usage: python3 <relative or absolute path to qlir>/quickstart/qlir_quickstart.py <project_name>")
        sys.exit(1)

    name = validate_project_name(sys.argv[1])
    dest = Path(name)
    if dest.exists():
        sys.exit(f"❌ destination exists: {dest}")

    # --- readme scaffold
    readme = dedent(f"""\
        # {name}

        🚀 **QLIR Quickstart Scaffold**

        This is a starter layout for a project that will use the QLIR library.

        ## ⚙️ Setup

        1. Clone or install QLIR somewhere accessible.
        2. Update the QLIR dependency in `pyproject.toml` before running install.

        ```toml
        # Option A: use local sibling path (edit as needed)
        # qlir = {{ path = "../qlir", develop = true }}

        # Option B: use a git reference (edit as needed)
        # qlir = {{ git = "https://github.com/Trones21/qlir.git", rev = "main" }}
        ```

        3. Install dependencies and run:
        ```bash
        poetry install
        poetry run python3 {name}/main.py
        ```

        ## 🧩 Structure
        ```
        {name}/
          main.py      # entry point
          analysis/    # your analysis modules
          data/        # optional data inputs
          outputs/
          tests/
        ```

        ## 📊 Data & Outputs

        When your workflow produces files under `outputs/`, consider including **metadata** about the dataset or parameters that generated them

        Good practice:
        - Include a small `outputs/README.md` describing the data source and run context.
        - If the underlying dataset is too large or private to check in, add a checksum or manifest.
        - For large binary artifacts, consider using [Git LFS](https://git-lfs.github.com/) (`git lfs install`) and track only the pointers.

        Everyone ends up evolving their own pattern here — the key is traceability.  
        If someone sees an output in your repo, they should be able to tell *what data and settings produced it*.

        ## 💬 Next steps
        - Chat with an LLM to decide how to structure your analysis pipeline
        - Or, explore examples in the QLIR repo under `examples/`
    """)

    # --- pyproject with commented placeholders
    pyproject = dedent(f"""\
    [project]
    name = "{name}"
    version = "0.1.0"
    description = "Starter project using QLIR"
    requires-python = ">=3.10, <4.0"
    readme = "README.md"
    dependencies = [
    "pandas>=2.2,<3.0",
    "numpy>=1.26,<2.0",
    # Local path dependency
    "qlir @ file:///home/tjr/gh/qlir",
    # or use the Git version:
    # "qlir @ git+https://github.com/Trones21/qlir.git@main",
    ]

    packages = [{{ include = "{name}", from = "src" }}]

    [project.optional-dependencies]
    dev = ["pytest>=8.0"]

    [tool.poetry.scripts]
    analysis = "{name}.main:entrypoint"

    [build-system]
    requires = ["poetry-core>=1.8.0"]
    build-backend = "poetry.core.masonry.api"
    """)

    main_py = dedent("""\
    import pandas as pd

    def entrypoint():
        print("Hello World!")
        print("Welcome to your QLIR analysis project, time to get to work!")
        return

        ToDo - copy from sma_slope=_persistence when it fully works        
        # A typical pipeline
        # 1. Get candles from disk or network
        # df = load_candles_from_disk(...)  # or qlir.fetch_* helpers

        # 2. Update dataset on disk with new candles (if this study requires that)
        # update_disk_dataset(df)

        # 3. Resample data (custom OHLCV candles)
        # df_resampled = build_custom_candles(df)

        # 4. Enrich and analyze
        # for now, assume df is ready:
        df = ...  # TODO: integrate your real pipeline, if you have many, you might consider creating a pipelines folder/module instead of placing it all here 
        main(df)


    def main(df: pd.DataFrame):
        \'''
        Core analysis body.

        - Add indicators / features / signals
        - Join with other datasets
        - Persist intermediate/final results
        - Optionally produce dataviz-friendly tables, plots, etc.
        \'''
        # e.g.:
        # df = add_sma_slope_features(df)
        # df = compute_slope_persistence_stats(df)
        # df.to_parquet("data/processed/slope_persistence.parquet")

        return df  # useful for tests / notebooks


    if __name__ == "__main__":
        entrypoint()

    """)

    gitignore = """\
        # Byte-compiled / cache
        __pycache__/
        *.py[cod]
        *.pyo
        *.pyd

        # Virtual environments
        .venv/
        env/
        venv/
        ENV/

        # Poetry / pip / build
        dist/
        build/
        *.egg-info/
        poetry.lock

        # Tests / coverage
        .pytest_cache/
        .coverage
        htmlcov/

        # IDEs / editors
        .vscode/
        .idea/
        *.swp

        # OS / misc
        .DS_Store
        Thumbs.db

        # Optional data conventions
        data/raw/
        data/tmp/
    """

    write(dest / ".gitignore", gitignore)


    write(dest / "README.md", readme)
    write(dest / "pyproject.toml", pyproject)
    write(dest / "src" / name / "__init__.py", "")
    write(dest / "src" / name / "main.py", main_py)
    write(dest / "tests" / "test_smoke.py", "def test_placeholder(): assert True\n")

    print(f"✅ Created starter project at {dest}")
    print("👉 Before installing, open pyproject.toml and update the qlir dependency path or git URL.")
    print("Then run:\n  cd", name, "\n  poetry install\n  poetry run analysis (default script in pyproject.toml) or poetry run python3 ", f"{name}/main.py")



def validate_project_name(name: str):
    """
    Validate the project name provided by the user.

    Rules:
    - Must be a valid Python package name.
    - Lowercase letters, digits, and underscores only.
    - Cannot start with a digit.
    - Cannot contain hyphens.
    - Cannot be a Python keyword.
    """

    if "-" in name:
        sys.exit("❌ Project name cannot contain hyphens. Use underscores instead.")

    if not re.match(r"^[a-z_][a-z0-9_]*$", name):
        sys.exit(
            "❌ Invalid project name.\n"
            "Allowed pattern: ^[a-z_][a-z0-9_]*$\n"
            "Use only lowercase letters, digits, and underscores."
        )

    if keyword.iskeyword(name):
        sys.exit(f"❌ '{name}' is a reserved Python keyword. Choose a different name.")

    # Passed all checks
    return name


if __name__ == "__main__":
    main()
