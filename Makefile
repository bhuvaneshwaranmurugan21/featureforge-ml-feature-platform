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
	python -m tools.run_stage1_proof --output evidence/stage1/temporal-proof.json
	python -m tools.run_stage2_proof --output-dir evidence/stage2
	python tools/validate_stage0.py
	python tools/validate_stage1.py
	python tools/validate_stage2.py
