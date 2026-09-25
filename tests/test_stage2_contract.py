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
        [sys.executable, str(target / "tools/validate_stage2.py"), "--root", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )


def _change(target: Path, name: str, edit: Any) -> None:
    path = target / name
    data = json.loads(path.read_text(encoding="utf-8"))
    edit(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_valid_stage2_contract_passes(tmp_path: Path) -> None:
    assert _run(_copy(tmp_path)).returncode == 0


def test_wrong_project_and_base_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage2/claims.json", lambda data: data.update(project="other"))
    assert "wrong project" in _run(target).stderr
    target = _copy(tmp_path / "base")
    _change(target, "evidence/stage2/baseline.json", lambda data: data.update(base_sha="bad"))
    assert "base SHA" in _run(target).stderr


def test_missing_requirement_and_overclaim_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(target, "docs/stage2/requirements.json", lambda data: data["requirements"].pop())
    assert "requirement" in _run(target).stderr
    target = _copy(tmp_path / "claim")
    _change(
        target,
        "docs/stage2/claims.json",
        lambda data: data["claims"][-1].update(label="LOCAL_VERIFIED"),
    )
    assert "overclaimed" in _run(target).stderr


def test_incomplete_fault_matrix_and_false_race_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    _change(
        target,
        "evidence/stage2/failure-recovery-proof.json",
        lambda data: data["fault_points"].pop(),
    )
    assert "fault matrix" in _run(target).stderr
    target = _copy(tmp_path / "race")
    _change(
        target,
        "evidence/stage2/failure-recovery-proof.json",
        lambda data: data["publication"]["race_results"].update({"generation-b": "committed"}),
    )
    assert "CAS proof" in _run(target).stderr


def test_stale_digest_and_oracle_production_import_fail(tmp_path: Path) -> None:
    target = _copy(tmp_path)
    path = target / "docs/stage2/lifecycle-specification.md"
    path.write_text(path.read_text(encoding="utf-8") + "\nstale\n", encoding="utf-8")
    assert "stale evidence digest" in _run(target).stderr
    target = _copy(tmp_path / "oracle")
    oracle = target / "tests/stage2_oracle.py"
    oracle.write_text(
        oracle.read_text(encoding="utf-8") + "\nfrom featureforge.temporal import select_events\n",
        encoding="utf-8",
    )
    _change(
        target,
        "evidence/stage2/vertical-slice-proof.json",
        lambda data: data["digests"].update(
            oracle_sha256=__import__("hashlib").sha256(oracle.read_bytes()).hexdigest()
        ),
    )
    assert "imports production" in _run(target).stderr
