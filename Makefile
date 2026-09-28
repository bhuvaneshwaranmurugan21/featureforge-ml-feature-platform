.PHONY: install test lint evidence

install:
	python -m pip install -e '.[dev,spark,aws]'

test:
	pytest

lint:
	ruff check .
	mypy src

evidence:
	python -m featureforge.cli simulate --output evidence/local-simulation.json
	python -m tools.run_stage1_proof --output evidence/stage1/temporal-proof.json
	python -m tools.run_stage2_proof --output-dir evidence/stage2
	python -m tools.run_stage3_proof --output-dir evidence/stage3
	python -m tools.run_stage4_proof --output-dir evidence/stage4
	python -m tools.run_stage5_proof --output-dir evidence/stage5
	python -m tools.summarize_stage5_benchmark --raw evidence/stage5/benchmark-raw.json --output evidence/stage5/benchmark-summary.json
	python tools/validate_stage0.py
	python tools/validate_stage1.py
	python tools/validate_stage2.py
	python tools/validate_stage3.py
	python tools/validate_stage4.py
	python tools/validate_stage5.py
