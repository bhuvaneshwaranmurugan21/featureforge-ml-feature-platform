.PHONY: install test lint evidence

install:
	python -m pip install -e '.[dev]'

test:
	pytest

lint:
	ruff check .
	mypy src

evidence:
	python -m featureforge.cli simulate --output evidence/local-simulation.json
