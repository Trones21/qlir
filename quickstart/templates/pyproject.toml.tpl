[project]
name = "__PROJECT_NAME__"
version = "0.1.0"
description = "Starter project using QLIR"
requires-python = ">=3.10, <4.0"
readme = "README.md"
dependencies = [
  "pandas>=2.2,<3.0",
  "numpy>=1.26,<2.0",
  # Local path dependency (edit as needed)
  "qlir @ file:///home/tjr/gh/qlir",
  # Or use the Git version:
  # "qlir @ git+https://github.com/Trones21/qlir.git@main",
]

packages = [{ include = "__PACKAGE_NAME__", from = "src" }]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[project.scripts]
analysis = "__PACKAGE_NAME__.main:entrypoint"

[build-system]
requires = ["poetry-core>=1.8.0"]
build-backend = "poetry.core.masonry.api"
