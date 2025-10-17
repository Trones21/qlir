.PHONY: setup test lint freeze clean

setup:
	python3 -m venv venv
	. venv/bin/activate && pip install -U pip
	. venv/bin/activate && pip install -e ".[dev]"

test:
	. venv/bin/activate && pytest

lint:
	. venv/bin/activate && ruff check src tests

freeze:
	. venv/bin/activate && pip freeze > requirements.txt

clean:
	rm -rf venv build dist *.egg-info .pytest_cache .ruff_cache
