#!/usr/bin/env python3
from pathlib import Path
import sys
import re
import keyword


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def templates_dir() -> Path:
    """Return the directory containing template files."""
    return Path(__file__).parent / "templates"


def render_template(template_name: str, substitutions: dict[str, str]) -> str:
    """
    Load a template file and apply simple token substitution.

    Templates should use __TOKEN__ placeholders, e.g. __PROJECT_NAME__.
    """
    template_path = templates_dir() / template_name
    if not template_path.exists():
        sys.exit(f"❌ missing template file: {template_path}")

    text = template_path.read_text(encoding="utf-8")
    for key, value in substitutions.items():
        text = text.replace(f"__{key}__", value)
    return text


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python3 qlir_quickstart.py <project_name>")
        sys.exit(1)

    name = validate_project_name(sys.argv[1])
    dest = Path(name)
    if dest.exists():
        sys.exit(f"❌ destination exists: {dest}")

    substitutions = {
        "PROJECT_NAME": name,
        "PACKAGE_NAME": name,
        # add more tokens later if you need them
    }

    # --- render from templates
    gitignore = render_template("gitignore.tpl", substitutions)
    readme = render_template("README.md.tpl", substitutions)
    pyproject = render_template("pyproject.toml.tpl", substitutions)
    main_py = render_template("main.py.tpl", substitutions)

    # --- write files
    write(dest / ".gitignore", gitignore)
    write(dest / "README.md", readme)
    write(dest / "pyproject.toml", pyproject)
    write(dest / "src" / name / "__init__.py", "")
    write(dest / "src" / name / "main.py", main_py)
    write(dest / "tests" / "test_smoke.py", "def test_placeholder(): assert True\n")

    print(f"✅ Created starter project at {dest}")
    print("👉 Before installing, open pyproject.toml and update the qlir dependency path or git URL.")
    print(
        "Then run:\n"
        f"  cd {name}\n"
        "  poetry install\n"
        "  poetry run analysis  # or: poetry run python -m __PACKAGE_NAME__.main\n"
    )


def validate_project_name(name: str) -> str:
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

    return name


if __name__ == "__main__":
    main()
