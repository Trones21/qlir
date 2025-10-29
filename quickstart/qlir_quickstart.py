#!/usr/bin/env python3
from pathlib import Path
from textwrap import dedent
import sys

def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def main():
    if len(sys.argv) < 2:
        print("usage: python qlir_quickstart.py <project_name>")
        sys.exit(1)

    name = sys.argv[1]
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
        # qlir = {{ git = "https://github.com/your-org/qlir.git", rev = "main" }}
        ```

        3. Install dependencies and run:
        ```bash
        poetry install
        poetry run python {name}/main.py
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

        When your workflow produces files under `outputs/`, consider including **metadata** about the dataset or parameters that generated them.

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
    requires-python = ">=3.10"
    readme = "README.md"
    dependencies = [
    "pandas>=2.2,<3.0",
    "numpy>=1.26,<2.0",
    # Local path dependency
    "qlir @ file:///home/tjr/gh/qlir",
    # or use the Git version:
    # "qlir @ git+https://github.com/your-org/qlir.git@main",
    ]

    [project.optional-dependencies]
    dev = ["pytest>=8.0"]

    [build-system]
    requires = ["poetry-core>=1.8.0"]
    build-backend = "poetry.core.masonry.api"
    """)

    main_py = dedent("""\
        import pandas as pd
        # placeholder imports — will work once qlir path is set up
        # import qlir.core.pointwise as pw
        # import qlir.core.bar_relations as br

        def run():
            df = pd.DataFrame({
                "open": [10,11,12,11,13],
                "close":[10.5,11,11.5,10.8,13],
            })
            print("data ready:", df.shape)
            print("✅ update pyproject.toml with your qlir path before running analysis")

        if __name__ == "__main__":
            run()
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
    write(dest / name / "__init__.py", "")
    write(dest / name / "main.py", main_py)
    write(dest / "tests" / "test_smoke.py", "def test_placeholder(): assert True\n")

    print(f"✅ Created starter project at {dest}")
    print("👉 Before installing, open pyproject.toml and update the qlir dependency path or git URL.")
    print("Then run:\n  cd", name, "\n  poetry install\n  poetry run python", f"{name}/main.py")

if __name__ == "__main__":
    main()
