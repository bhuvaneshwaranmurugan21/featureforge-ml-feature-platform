"""Adversarial checks for the Stage 4 fail-closed evidence validator."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _copy(tmp_path: Path) -> Path:
    target = tmp_path / "repo"
    shutil.copytree(
        ROOT,
        target,
        ignore=shutil.ignore_patterns(
            ".git",
            ".hypothesis",
            ".mypy_cache",
            ".pytest_cache",
            ".coverage",
            "*.egg-info",
            "__pycache__",
        ),
    )
    return target


def _run(target: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(target / "tools/validate_stage4.py"), "--root", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )


def _change(target: Path, name: str, edit: Any) -> None:
    path = target / name
    data = json.loads(path.read_text(encoding="utf-8"))
    edit(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def _digest(value: Any) -> str:
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def test_valid_stage4_contract_passes(tmp_path: Path) -> None:
    assert _run(_copy(tmp_path)).returncode == 0


def test_missing_acceptance_and_runtime_overclaim_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(
        target,
        "docs/stage4/requirements.json",
        lambda data: data["acceptance_criteria"].pop(),
    )
    assert "ST4-AC-01 through ST4-AC-24" in _run(target).stderr
    target = _copy(tmp_path / "runtime")
    _change(
        target,
        "evidence/stage4/baseline.json",
        lambda data: data.update(aws_runtime_executed=True),
    )
    assert "unexpected AWS runtime claim" in _run(target).stderr


def test_false_proof_and_oracle_production_import_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)

    def falsify(data: dict[str, Any]) -> None:
        data["checks"]["request_shapes_valid"] = False
        data.pop("proof_digest")
        data["proof_digest"] = _digest(data)

    _change(target, "evidence/stage4/online-proof.json", falsify)
    assert "online proof check failed" in _run(target).stderr
    target = _copy(tmp_path / "oracle")
    oracle = target / "tests/stage4_oracle.py"
    oracle.write_text(
        oracle.read_text(encoding="utf-8")
        + "\nfrom featureforge.online import build_materialization_plan\n",
        encoding="utf-8",
    )
    assert "oracle imports production" in _run(target).stderr


def test_stale_index_and_contamination_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    specification = target / "docs/stage4/online-materialization-specification.md"
    specification.write_text(
        specification.read_text(encoding="utf-8") + "\nstale\n", encoding="utf-8"
    )
    assert "evidence index is stale" in _run(target).stderr
    target = _copy(tmp_path / "contamination")
    source = target / "src/featureforge/online.py"
    source.write_text(
        source.read_text(encoding="utf-8") + "\n# ledgerguard\n", encoding="utf-8"
    )
    assert "other-project contamination" in _run(target).stderr
