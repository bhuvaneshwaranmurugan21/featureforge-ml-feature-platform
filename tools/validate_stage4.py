#!/usr/bin/env python3
"""Fail-closed validator for Part 2 Stage 2 / global Stage 4."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from featureforge.canonical import digest

PROJECT = "featureforge-ml-feature-platform"
BASE = "15fbe0a66a74cd340a717266fb80ee36388411bc"
BASE_TREE = "437f33ed2f0c5aa469bd5c52a741a675e849144a"
ACCEPTANCE = {f"ST4-AC-{number:02d}" for number in range(1, 25)}
INDEXED = (
    ".github/workflows/ci.yml",
    "Makefile",
    "README.md",
    "contracts/online-activation-receipt-v1.json",
    "contracts/online-active-pointer-v1.json",
    "contracts/online-feature-record-v1.json",
    "contracts/online-materialization-plan-v1.json",
    "contracts/online-serving-result-v1.json",
    "contracts/online-validation-receipt-v1.json",
    "docs/architecture.md",
    "docs/claims.yaml",
    "docs/runbook.md",
    "docs/stage4/claims.json",
    "docs/stage4/failure-repair-log.md",
    "docs/stage4/online-materialization-specification.md",
    "docs/stage4/predecessor.json",
    "docs/stage4/requirements.json",
    "docs/stage4/serving-decision-table.json",
    "docs/stage4/state-transition-table.json",
    "docs/stage4/traceability.json",
    "docs/stage4/walkthrough.md",
    "evidence/stage4/baseline.json",
    "evidence/stage4/failure-recovery-proof.json",
    "evidence/stage4/online-proof.json",
    "pyproject.toml",
    "requirements-dev.lock",
    "src/featureforge/online.py",
    "tests/stage4_oracle.py",
    "tests/test_stage4_contract.py",
    "tests/test_stage4_online.py",
    "tools/run_stage4_proof.py",
    "tools/validate_stage4.py",
)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Stage 4 validation failed: {message}")


def _load(root: Path, name: str) -> dict[str, Any]:
    value: dict[str, Any] = json.loads((root / name).read_text(encoding="utf-8"))
    return value


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _proof_digest(value: dict[str, Any]) -> str:
    payload = dict(value)
    recorded = payload.pop("proof_digest", None)
    _check(isinstance(recorded, str), "proof digest missing")
    _check(recorded == digest(payload), "proof digest does not bind proof content")
    return recorded


def _build_index(root: Path) -> dict[str, Any]:
    return {
        "project": PROJECT,
        "base_sha": BASE,
        "base_tree": BASE_TREE,
        "files_sha256": {name: _sha(root / name) for name in INDEXED},
    }


def _validate_oracle_independence(root: Path) -> None:
    source = (root / "tests/stage4_oracle.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    _check(
        not any(name == "featureforge" or name.startswith("featureforge.") for name in imports),
        "independent oracle imports production FeatureForge logic",
    )


def _validate_contracts(root: Path) -> None:
    expected = {
        "contracts/online-activation-receipt-v1.json": (
            "urn:featureforge:online-activation-receipt:v1"
        ),
        "contracts/online-active-pointer-v1.json": "urn:featureforge:online-active-pointer:v1",
        "contracts/online-feature-record-v1.json": "urn:featureforge:online-feature-record:v1",
        "contracts/online-materialization-plan-v1.json": (
            "urn:featureforge:online-materialization-plan:v1"
        ),
        "contracts/online-serving-result-v1.json": "urn:featureforge:online-serving-result:v1",
        "contracts/online-validation-receipt-v1.json": (
            "urn:featureforge:online-validation-receipt:v1"
        ),
    }
    for name, identity in expected.items():
        _check(_load(root, name).get("$id") == identity, f"contract identity mismatch: {name}")


def _validate_proofs(root: Path) -> None:
    online = _load(root, "evidence/stage4/online-proof.json")
    recovery = _load(root, "evidence/stage4/failure-recovery-proof.json")
    _proof_digest(online)
    _proof_digest(recovery)
    _check(online.get("project") == PROJECT, "online proof project mismatch")
    _check(recovery.get("project") == PROJECT, "recovery proof project mismatch")
    _check(
        online.get("contract") == "featureforge-stage4-online-proof-v1",
        "online proof contract mismatch",
    )
    _check(
        recovery.get("contract") == "featureforge-stage4-recovery-proof-v1",
        "recovery proof contract mismatch",
    )
    _check(online.get("checks") and all(online["checks"].values()), "online proof check failed")
    _check(
        recovery.get("checks") and all(recovery["checks"].values()),
        "recovery proof check failed",
    )
    runtime = online.get("runtime", {})
    pins = {
        "boto3": "1.43.103",
        "botocore": "1.43.103",
        "jmespath": "1.1.0",
        "s3transfer": "0.19.2",
    }
    _check(all(runtime.get(name) == version for name, version in pins.items()), "SDK proof pins")
    plan = online.get("plan", {})
    _check(plan.get("expected_count") == 15, "golden generation must contain 15 records")
    _check(online.get("pagination_page_counts") == [4, 4, 4, 3], "pagination proof mismatch")
    request = online.get("request_model", {})
    _check(request.get("all_service_model_valid") is True, "request validation failed")
    _check(request.get("transaction_action_count") == 3, "activation action count mismatch")
    profiles = online.get("capacity_profiles", [])
    _check(
        [row.get("profile") for row in profiles]
        == ["balanced", "skewed", "high-cardinality"],
        "capacity profile sequence mismatch",
    )
    _check(
        [row.get("reconciliation_record_count") for row in profiles] == [60, 120, 200],
        "capacity record counts mismatch",
    )
    for profile in profiles:
        report = profile.get("report", {})
        _check(report.get("maximum_item_headroom_bytes", 0) > 0, "item limit exceeded")
        _check(report.get("maximum_records_per_partition") == 5, "partition envelope drift")
    race = recovery.get("race", {})
    _check(
        race.get("outcomes") == ["committed", "semantic-conflict"],
        "two-publisher CAS proof mismatch",
    )
    _check(race.get("pointer_version") == 2, "race advanced pointer more than once")
    _check(recovery.get("retry", {}).get("delays_ms") == [135, 215, 464], "retry drift")
    _check(
        recovery.get("error_classification")
        == {
            "access_denied": "TERMINAL",
            "conditional": "CONFLICT",
            "malformed_unknown": "UNKNOWN",
            "throttle": "RETRYABLE",
        },
        "error classification drift",
    )


def validate(root: Path, expect_head: str | None = None) -> None:
    requirements = _load(root, "docs/stage4/requirements.json")
    ids = {row["id"] for row in requirements.get("acceptance_criteria", [])}
    _check(ids == ACCEPTANCE, "requirements must contain exactly ST4-AC-01 through ST4-AC-24")
    traceability = _load(root, "docs/stage4/traceability.json")
    mapped = {row["acceptance"] for row in traceability.get("mappings", [])}
    _check(mapped == ACCEPTANCE, "traceability must map every and only Stage 4 criterion")

    predecessor = _load(root, "docs/stage4/predecessor.json")
    _check(predecessor.get("project") == PROJECT, "predecessor project mismatch")
    _check(predecessor.get("authorized_base_commit") == BASE, "authorized base mismatch")
    _check(predecessor.get("authorized_base_tree") == BASE_TREE, "authorized tree mismatch")
    _check(predecessor.get("predecessor_status") == "verified-complete", "predecessor open")
    baseline = _load(root, "evidence/stage4/baseline.json")
    _check(baseline.get("authorized_base_commit") == BASE, "baseline base mismatch")
    _check(baseline.get("authorized_base_tree") == BASE_TREE, "baseline tree mismatch")
    _check(baseline.get("predecessor_gates_passed") is True, "predecessor gates failed")
    _check(baseline.get("aws_runtime_executed") is False, "unexpected AWS runtime claim")

    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    lock = (root / "requirements-dev.lock").read_text(encoding="utf-8")
    for pin in (
        "boto3==1.43.103",
        "botocore==1.43.103",
        "jmespath==1.1.0",
        "s3transfer==0.19.2",
    ):
        _check(pin in pyproject, f"project AWS dependency missing {pin}")
        _check(pin in lock, f"development lock missing {pin}")
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    _check(
        ".[dev,spark,aws]" in workflow or ".[aws]" in workflow,
        "CI does not install the AWS extra",
    )
    _check("validate_stage4.py" in workflow, "CI does not run the Stage 4 validator")
    _check("run_stage4_proof" in workflow, "CI does not regenerate Stage 4 proofs")

    _validate_contracts(root)
    _validate_oracle_independence(root)
    _validate_proofs(root)
    _check(
        _sha(root / "evidence/stage3/spark-proof.json")
        == "58e6ba459060aca962398d056bf2f16c3e13eee55c155134661578b1f64d2d72",
        "Stage 3 Spark proof changed",
    )
    _check(
        _sha(root / "evidence/stage3/failure-recovery-proof.json")
        == "62c2f6e55c9964c8fe309d6f2d093ac2f5ee7b33575b69b274c496b6dc09e4c7",
        "Stage 3 recovery proof changed",
    )

    claims = _load(root, "docs/stage4/claims.json")
    _check(len(claims.get("explicit_non_claims", [])) >= 4, "Stage 4 non-claims incomplete")
    contamination_files = (
        "src/featureforge/online.py",
        "tests/stage4_oracle.py",
        "tests/test_stage4_online.py",
        "tools/run_stage4_proof.py",
        "docs/stage4/online-materialization-specification.md",
        "evidence/stage4/online-proof.json",
    )
    rendered = "\n".join(
        (root / name).read_text(encoding="utf-8").lower() for name in contamination_files
    )
    forbidden = ("ledgerguard", "changebridge", "eventpulse", "atlas retail")
    _check(not any(name in rendered for name in forbidden), "other-project contamination")
    source = (root / "src/featureforge/online.py").read_text(encoding="utf-8")
    _check("boto3.client(" not in source, "runtime AWS client construction is forbidden")
    if (root / ".git").exists():
        tracked = subprocess.run(
            ["git", "ls-files"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    else:
        ignored_parts = {"__pycache__", ".pytest_cache", ".mypy_cache", ".hypothesis"}
        tracked = [
            str(path.relative_to(root))
            for path in root.rglob("*")
            if path.is_file() and not ignored_parts.intersection(path.parts)
        ]
    generated = tuple(
        name
        for name in tracked
        if Path(name).suffix in {".sqlite", ".db", ".pyc"}
        or Path(name).name == ".coverage"
    )
    _check(not generated, "generated database, bytecode, or coverage artifact is tracked locally")

    index = _load(root, "evidence/stage4/evidence-index.json")
    _check(index == _build_index(root), "Stage 4 evidence index is stale or incomplete")
    if expect_head is not None:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        _check(head == expect_head, "repository HEAD differs from expected exact head")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write-index", action="store_true")
    parser.add_argument("--expect-head")
    args = parser.parse_args()
    if args.write_index:
        target = args.root / "evidence/stage4/evidence-index.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(_build_index(args.root), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {target}")
        return
    validate(args.root, args.expect_head)
    print(
        "Stage 4 validation passed: ST4-AC-01 through ST4-AC-23 local gates are closed; "
        "ST4-AC-24 is ready for exact-head CI, review, merge, and continuation closure."
    )


if __name__ == "__main__":
    main()
