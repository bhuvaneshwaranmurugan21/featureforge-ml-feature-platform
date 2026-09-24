"""Adversarial checks that the Stage 0 evidence validator fails closed."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _copy(tmp_path: Path) -> Path:
    target = tmp_path / "repo"
    shutil.copytree(
        ROOT,
        target,
        ignore=shutil.ignore_patterns(
            ".git", ".pytest_cache", ".coverage", "*.egg-info", "__pycache__"
        ),
    )
    return target


def _run(target: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(target / "tools/validate_stage0.py"), "--root", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )


def _change(target: Path, name: str, edit: object) -> None:
    path = target / name
    data = json.loads(path.read_text(encoding="utf-8"))
    edit(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_valid_contract_passes(tmp_path: Path) -> None:
    assert _run(_copy(tmp_path)).returncode == 0


def test_missing_requirement_fails(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage0/requirements.json", lambda data: data["requirements"].pop())
    assert "requirement" in _run(target).stderr


def test_unsupported_aws_claim_fails(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(
        target,
        "docs/stage0/claims.json",
        lambda data: data["claims"][0].update(label="AWS_VERIFIED"),
    )
    assert "unsupported Stage 0" in _run(target).stderr


def test_wrong_project_and_stale_base_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage0/inventory.json", lambda data: data.update(project="other-project"))
    assert "wrong project" in _run(target).stderr
    target = _copy(tmp_path / "second")
    _change(target, "docs/stage0/requirements.json", lambda data: data.update(base_sha="0" * 40))
    assert "stale or wrong base" in _run(target).stderr


def test_changed_artifact_digest_fails(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    path = target / "docs/stage0/risk-register.md"
    path.write_text(path.read_text(encoding="utf-8") + "\nchanged\n", encoding="utf-8")
    assert "stale evidence digest" in _run(target).stderr
