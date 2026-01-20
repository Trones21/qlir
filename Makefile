# Makefile
# Usage examples:
#   make install         # poetry install (with dev deps)
#   make run ARGS="--help"
#   make cli ARGS="--help"
#   make test
#   make test-file f=tests/test_something.py
#   make test-func f=tests/test_something.py::test_case
#   make lint
#   make format
#   make build
#   make clean

.PHONY: install run cli test test-file test-func lint format check freeze build publish clean distclean help

# ----- Install / Setup -----
install:
	poetry install --with dev

# ----- Run (module or console script) -----
# Run as a module (doesn't require script entrypoint)
run:  ## e.g. make run ARGS="--help"
	poetry run python -m qlir.cli $(ARGS)

# Run the console script (after enabling [project.scripts] qlir=... in pyproject.toml)
cli:  ## e.g. make cli ARGS="--help"
	poetry run qlir $(ARGS)

# ----- Testing -----
test:
	poetry run pytest

test-network:
	poetry run pytest -m network
	
test-local:
	poetry run pytest -m local
#file/func
test-f:
	@if [ -z "$(f)" ]; then echo "Filter tests to file or func Usage: make test-f f=path/to/test_file.py or make test-f f=path/to/test_file.py::test_func"; exit 1; fi
	poetry run pytest $(f)

# file/func with print enabled (-s)
test-f-wp:
	@if [ -z "-s $(f)" ]; then echo "Filter tests to file or func and run with print enabled (-s) Usage: make test-f-wp f=path/to/test_file.py or make test-f f=path/to/test_file.py::test_func"; exit 1; fi
	poetry run pytest $(f)
# make test-f-wp f="./tests/data/test_drift_using_imported_openapi_created_lib.py::test_loop_candles"

test-analysis-server:
	poetry run pytest tests/servers/analysis_server
# conftest.py mark application... mark is being applied but tests filtered out idk why (path isnt in addopts...) so just use path 
# according to chatgpt: Directory-level pytestmark is not guaranteed to be attached early enough to satisfy -m

test-notification-server:
	poetry run pytest tests/servers/notification_server

# ----- Quality -----
lint:
	poetry run ruff check src tests

format:
	poetry run ruff format src tests
	poetry run ruff check --fix src tests

check: lint test

# ----- Freeze (optional) -----
# Only useful if you want a requirements.txt alongside Poetry
freeze:
	poetry run pip freeze > requirements.txt

# ----- Build / Publish -----
build:
	poetry run python -m pip install -U build twine
	poetry run python -m build
	poetry run twine check dist/*

publish: build
	poetry run twine upload dist/*

# ----- Cleanup -----
clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache **/__pycache__

distclean: clean
	rm -rf .venv

help:
	@echo "Targets: install run cli test test-file test-func lint format check freeze build publish clean distclean"
