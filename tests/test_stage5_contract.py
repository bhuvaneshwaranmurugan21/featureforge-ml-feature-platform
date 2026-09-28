"""Adversarial checks for the Stage 5 fail-closed evidence validator."""

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
        [sys.executable, str(target / "tools/validate_stage5.py"), "--root", str(target)],
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


def test_valid_stage5_contract_passes(tmp_path: Path) -> None:
    assert _run(_copy(tmp_path)).returncode == 0


def test_missing_acceptance_and_runtime_overclaim_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage5/requirements.json", lambda data: data["acceptance_criteria"].pop())
    assert "ST5-AC-01 through ST5-AC-25" in _run(target).stderr
    target = _copy(tmp_path / "runtime")
    _change(
        target,
        "evidence/stage5/baseline.json",
        lambda data: data.update(aws_runtime_executed=True),
    )
    assert "unexpected AWS runtime claim" in _run(target).stderr


def test_false_proof_and_projector_production_import_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)

    def falsify(data: dict[str, Any]) -> None:
        data["checks"]["exhaustive_parity_passed"] = False
        data.pop("proof_digest")
        data["proof_digest"] = _digest(data)

    _change(target, "evidence/stage5/platform-proof.json", falsify)
    assert "platform proof check failed" in _run(target).stderr
    target = _copy(tmp_path / "oracle")
    projector = target / "src/featureforge/expected.py"
    projector.write_text(
        projector.read_text(encoding="utf-8")
        + "\nfrom featureforge.online import build_materialization_plan\n",
        encoding="utf-8",
    )
    assert "projector imports production" in _run(target).stderr


def test_raw_trial_tamper_stale_index_and_contamination_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(
        target,
        "evidence/stage5/benchmark-raw.json",
        lambda data: data["trials"][0].update(duration_ns=1),
    )
    assert "raw_digest does not bind content" in _run(target).stderr
    target = _copy(tmp_path / "index")
    specification = target / "docs/stage5/parity-activation-specification.md"
    specification.write_text(
        specification.read_text(encoding="utf-8") + "\nstale\n", encoding="utf-8"
    )
    assert "evidence index is stale" in _run(target).stderr
    target = _copy(tmp_path / "contamination")
    source = target / "src/featureforge/assurance.py"
    source.write_text(source.read_text(encoding="utf-8") + "\n# ledgerguard\n", encoding="utf-8")
    assert "other-project contamination" in _run(target).stderr
