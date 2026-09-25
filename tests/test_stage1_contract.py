"""Adversarial checks for the Stage 1 evidence validator."""

from __future__ import annotations

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
        [sys.executable, str(target / "tools/validate_stage1.py"), "--root", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )


def _change(target: Path, name: str, edit: Any) -> None:
    path = target / name
    data = json.loads(path.read_text(encoding="utf-8"))
    edit(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_valid_stage1_contract_passes(tmp_path: Path) -> None:
    assert _run(_copy(tmp_path)).returncode == 0


def test_missing_requirement_and_wrong_project_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage1/requirements.json", lambda data: data["requirements"].pop())
    assert "requirement" in _run(target).stderr
    target = _copy(tmp_path / "wrong-project")
    _change(target, "docs/stage1/claims.json", lambda data: data.update(project="other-project"))
    assert "wrong project" in _run(target).stderr


def test_false_proof_and_unsupported_claim_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "evidence/stage1/temporal-proof.json", lambda data: data.update(result="FAIL"))
    assert "proof did not pass" in _run(target).stderr
    target = _copy(tmp_path / "claim")
    _change(
        target,
        "docs/stage1/claims.json",
        lambda data: data["claims"][0].update(label="AWS_VERIFIED"),
    )
    assert "unsupported claim" in _run(target).stderr


def test_stale_digest_and_missing_negative_control_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    path = target / "docs/stage1/temporal-specification.md"
    path.write_text(path.read_text(encoding="utf-8") + "\nstale\n", encoding="utf-8")
    assert "stale proof digest" in _run(target).stderr
    target = _copy(tmp_path / "control")
    _change(
        target,
        "evidence/stage1/temporal-proof.json",
        lambda data: data["negative_controls"].update(leaky_event_time_only_detected=False),
    )
    assert "negative control" in _run(target).stderr
