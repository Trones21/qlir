# Makefile
# Usage examples:
#   make dev           # create venv + install with [dev]
#   make run ARGS="--help"
#   make format        # ruff format + autofix
#   make test          # pytest
#   make build         # python -m build
#   make clean         # remove caches, build artifacts, venv

.PHONY: dev install venv run cli test lint format check freeze build publish clean distclean help

VENV ?= venv
PY    = $(VENV)/bin/python
RUFF  = $(VENV)/bin/ruff
PYT   = $(VENV)/bin/pytest
TWINE = $(VENV)/bin/twine


install: poetry install --extras dev

# ----- Run (module or console script) -----
# Run as a module (works even without entry point)
run:  ## e.g. make run ARGS="--help"
	$(PY) -m qlir.cli $(ARGS)

# Run the console script (after enabling [project.scripts] qlir=...)
cli:  ## e.g. make cli ARGS="--help"
	$(VENV)/bin/qlir $(ARGS)

# ----- Quality -----
test:
	$(PYT)

test-file:
	@if [ -z "$(f)" ]; then echo "Usage: make test-file f=path/to/test_file.py"; exit 1; fi
	$(PYT) $(f)

test-func:
	@if [ -z "$(f)" ]; then echo "Usage: make test-func f=path/to/test_file.py::test_func"; exit 1; fi
	$(PYT) $(f)


lint:
	$(RUFF) check src tests

format:
	$(RUFF) format src tests
	$(RUFF) check --fix src tests

check: lint test

freeze:
	$(PIP) freeze > requirements.txt

# ----- Build / Publish -----
build:
	$(PY) -m pip install -U build twine
	$(PY) -m build
	$(TWINE) check dist/*

publish: build
	$(TWINE) upload dist/*

# ----- Cleanup -----
clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache **/__pycache__

distclean: clean
	rm -rf $(VENV)

help:
	@echo "Targets: dev install venv run cli test lint format check freeze build publish clean distclean"
