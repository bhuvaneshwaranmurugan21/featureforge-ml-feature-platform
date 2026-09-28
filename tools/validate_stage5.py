#!/usr/bin/env python3
"""Fail-closed validator for Part 2 Stage 3 / global Stage 5."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from featureforge.assurance import BenchmarkTrial, summarize_trials
from featureforge.canonical import canonical_json, digest

PROJECT = "featureforge-ml-feature-platform"
BASE = "937e266f1f3777467d4abac5da861604c4ea9393"
BASE_TREE = "1c6a48fb96c2f5838a3bb46d410a36c03c85ae4f"
ACCEPTANCE = {f"ST5-AC-{number:02d}" for number in range(1, 26)}
OPERATIONS = {
    "workload_input_validation",
    "spark_full_rebuild",
    "affected_scope_planning",
    "spark_incremental_rebuild",
    "artifact_roundtrip",
    "online_materialize_reconcile",
    "independent_parity",
    "activation_decision_cas",
    "generation_pinned_reads",
}
INDEXED = (
    ".github/workflows/ci.yml",
    "Makefile",
    "README.md",
    "contracts/online-activation-receipt-v2.json",
    "contracts/stage5-activation-decision-v1.json",
    "contracts/stage5-benchmark-trial-v1.json",
    "contracts/stage5-parity-report-v1.json",
    "contracts/stage5-telemetry-event-v1.json",
    "docs/architecture.md",
    "docs/claims.yaml",
    "docs/runbook.md",
    "docs/stage5/benchmark-protocol.json",
    "docs/stage5/claims.json",
    "docs/stage5/diagnosis-decision-table.json",
    "docs/stage5/failure-repair-log.md",
    "docs/stage5/freshness-observability-specification.md",
    "docs/stage5/parity-activation-specification.md",
    "docs/stage5/performance-analysis.md",
    "docs/stage5/predecessor.json",
    "docs/stage5/requirements.json",
    "docs/stage5/traceability.json",
    "docs/stage5/walkthrough.md",
    "evidence/stage5/baseline.json",
    "evidence/stage5/benchmark-raw.json",
    "evidence/stage5/benchmark-summary.json",
    "evidence/stage5/failure-recovery-proof.json",
    "evidence/stage5/platform-proof.json",
    "src/featureforge/assurance.py",
    "src/featureforge/expected.py",
    "src/featureforge/online.py",
    "tests/test_stage5_assurance.py",
    "tests/test_stage5_contract.py",
    "tests/test_stage5_expected.py",
    "tools/run_stage5_benchmark.py",
    "tools/run_stage5_proof.py",
    "tools/summarize_stage5_benchmark.py",
    "tools/validate_stage5.py",
)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Stage 5 validation failed: {message}")


def _load(root: Path, name: str) -> dict[str, Any]:
    value: dict[str, Any] = json.loads((root / name).read_text(encoding="utf-8"))
    return value


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_embedded_digest(value: dict[str, Any], field: str) -> str:
    body = dict(value)
    recorded = body.pop(field, None)
    _check(isinstance(recorded, str), f"{field} missing")
    _check(recorded == digest(body), f"{field} does not bind content")
    return recorded


def _build_index(root: Path) -> dict[str, Any]:
    return {
        "base_sha": BASE,
        "base_tree": BASE_TREE,
        "files_sha256": {name: _sha(root / name) for name in INDEXED},
        "project": PROJECT,
    }


def _validate_independence(root: Path) -> None:
    tree = ast.parse((root / "src/featureforge/expected.py").read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    _check(
        not any(name == "featureforge" or name.startswith("featureforge.") for name in imports),
        "independent projector imports production FeatureForge logic",
    )


def _validate_contracts(root: Path) -> None:
    expected = {
        "contracts/online-activation-receipt-v2.json": (
            "urn:featureforge:online-activation-receipt:v2"
        ),
        "contracts/stage5-activation-decision-v1.json": (
            "urn:featureforge:stage5-activation-decision:v1"
        ),
        "contracts/stage5-benchmark-trial-v1.json": (
            "urn:featureforge:stage5-benchmark-trial:v1"
        ),
        "contracts/stage5-parity-report-v1.json": (
            "urn:featureforge:stage5-parity-report:v1"
        ),
        "contracts/stage5-telemetry-event-v1.json": (
            "urn:featureforge:stage5-telemetry-event:v1"
        ),
    }
    for name, identity in expected.items():
        _check(_load(root, name).get("$id") == identity, f"contract identity mismatch: {name}")


def _validate_proofs(root: Path) -> None:
    platform = _load(root, "evidence/stage5/platform-proof.json")
    recovery = _load(root, "evidence/stage5/failure-recovery-proof.json")
    _verify_embedded_digest(platform, "proof_digest")
    _verify_embedded_digest(recovery, "proof_digest")
    _check(platform.get("project") == PROJECT, "platform proof project mismatch")
    _check(recovery.get("project") == PROJECT, "recovery proof project mismatch")
    _check(
        platform.get("checks") and all(platform["checks"].values()),
        "platform proof check failed",
    )
    _check(
        recovery.get("checks") and all(recovery["checks"].values()),
        "failure/recovery proof check failed",
    )
    gate_failures = recovery.get("gate_failures", {})
    _check(len(gate_failures) == 9, "failure proof does not cover all nine gates")
    _check(
        platform.get("activation", {}).get("receipt", {}).get("contract")
        == "online-activation-receipt-v2",
        "activation receipt is not policy-versioned",
    )


def _validate_benchmark(root: Path) -> None:
    protocol = _load(root, "docs/stage5/benchmark-protocol.json")
    raw = _load(root, "evidence/stage5/benchmark-raw.json")
    summary = _load(root, "evidence/stage5/benchmark-summary.json")
    raw_digest = _verify_embedded_digest(raw, "raw_digest")
    _check(raw.get("protocol_digest") == digest(protocol), "raw protocol identity mismatch")
    _check(raw.get("trial_count") == 486, "benchmark must retain exactly 486 trials")
    _check(len(raw.get("cases", [])) == 9, "benchmark must retain the complete 3x3 matrix")
    _check(
        {row.get("profile") for row in raw["cases"]}
        == {"balanced", "skewed", "high-cardinality"},
        "benchmark profile coverage is incomplete",
    )
    _check(
        {row.get("size_class") for row in raw["cases"]} == {"small", "medium", "large"},
        "benchmark size coverage is incomplete",
    )
    _check(
        {row.get("incremental_mode") for row in raw["cases"]} == {"full", "incremental"},
        "benchmark does not prove incremental and fallback modes",
    )
    trials: list[BenchmarkTrial] = []
    grouped: dict[tuple[str, str], list[BenchmarkTrial]] = {}
    for value in raw.get("trials", []):
        body = dict(value)
        recorded = body.pop("trial_digest", None)
        contract = body.pop("contract", None)
        _check(contract == "stage5-benchmark-trial-v1", "trial contract mismatch")
        _check(
            recorded == digest({"contract": contract, **body}),
            "trial digest does not bind content",
        )
        trial = BenchmarkTrial(**body)
        _check(trial.correctness_passed, "incorrect trial was retained")
        trials.append(trial)
        grouped.setdefault((trial.case_id, trial.operation), []).append(trial)
    _check({trial.operation for trial in trials} == OPERATIONS, "operation coverage drift")
    _check(len(grouped) == 81, "benchmark case/operation matrix is incomplete")
    for rows in grouped.values():
        _check(
            sum(not row.warm for row in rows) == 1 and sum(row.warm for row in rows) == 5,
            "benchmark group must retain one cold and five warm trials",
        )
        _check(len({row.input_digest for row in rows}) == 1, "trial input identity drift")
        _check(len({row.output_digest for row in rows}) == 1, "trial output identity drift")
    derived = summarize_trials(trials)
    _verify_embedded_digest(summary, "report_digest")
    _check(summary.get("raw_digest") == raw_digest, "summary is not bound to raw evidence")
    _check(summary.get("summary") == derived, "benchmark summary derivation is stale")
    _check(summary.get("profile_count") == 3, "summary profile count mismatch")
    _check(summary.get("size_class_count") == 3, "summary size count mismatch")


def validate(root: Path, expect_head: str | None = None) -> None:
    requirements = _load(root, "docs/stage5/requirements.json")
    ids = {row["id"] for row in requirements.get("acceptance_criteria", [])}
    _check(ids == ACCEPTANCE, "requirements must contain exactly ST5-AC-01 through ST5-AC-25")
    traceability = _load(root, "docs/stage5/traceability.json")
    mapped = {row["acceptance"] for row in traceability.get("mappings", [])}
    _check(mapped == ACCEPTANCE, "traceability must map every and only Stage 5 criterion")
    predecessor = _load(root, "docs/stage5/predecessor.json")
    baseline = _load(root, "evidence/stage5/baseline.json")
    for value in (predecessor, baseline):
        _check(value.get("authorized_base_commit") == BASE, "authorized base mismatch")
        _check(value.get("authorized_base_tree") == BASE_TREE, "authorized tree mismatch")
    _check(predecessor.get("predecessor_status") == "verified-complete", "predecessor open")
    _check(baseline.get("predecessor_gates_passed") is True, "predecessor gates failed")
    _check(baseline.get("new_dependency_added") is False, "unauthorized dependency added")
    _check(baseline.get("aws_runtime_executed") is False, "unexpected AWS runtime claim")
    _check(baseline.get("dynamodb_runtime_executed") is False, "unexpected DynamoDB runtime")
    _check(
        _sha(root / "evidence/stage4/online-proof.json")
        == "a6d54df92b44afc0242611eedb4cfe09512f328fb8764e40101fe3bf1ce36b12",
        "Stage 4 online proof changed",
    )
    _check(
        _sha(root / "evidence/stage4/failure-recovery-proof.json")
        == "14c4a3df203dea66378c60924c67ebcbdb676cd77818be2bdf95593b5af385b2",
        "Stage 4 recovery proof changed",
    )
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    _check("validate_stage5.py" in workflow, "CI does not run Stage 5 validation")
    _check("run_stage5_proof" in workflow, "CI does not regenerate Stage 5 proofs")
    _check(
        "summarize_stage5_benchmark" in workflow,
        "CI does not reproduce the benchmark summary",
    )
    _validate_contracts(root)
    _validate_independence(root)
    _validate_proofs(root)
    _validate_benchmark(root)
    claims = _load(root, "docs/stage5/claims.json")
    classes = {row.get("class") for row in claims.get("claims", [])}
    _check(
        classes == {"LOCAL_VERIFIED", "MEASURED", "DESIGN_ONLY", "UNCLAIMED"},
        "claim classifications are incomplete",
    )
    _check(len(claims.get("explicit_non_claims", [])) >= 4, "non-claims incomplete")
    contamination_files = (
        "src/featureforge/assurance.py",
        "src/featureforge/expected.py",
        "tests/test_stage5_assurance.py",
        "tests/test_stage5_expected.py",
        "docs/stage5/parity-activation-specification.md",
        "evidence/stage5/platform-proof.json",
    )
    rendered = "\n".join(
        (root / name).read_text(encoding="utf-8").lower() for name in contamination_files
    )
    forbidden_projects = ("ledgerguard", "changebridge", "eventpulse", "atlas retail")
    _check(
        not any(name in rendered for name in forbidden_projects),
        "other-project contamination",
    )
    if (root / ".git").exists():
        tracked = subprocess.run(
            ["git", "ls-files"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.splitlines()
        generated = tuple(
            name
            for name in tracked
            if Path(name).suffix in {".sqlite", ".db", ".pyc"}
            or Path(name).name == ".coverage"
            or Path(name).parts[:1] == ("build",)
        )
        _check(not generated, "generated database, bytecode, or coverage artifact is tracked")
    index = _load(root, "evidence/stage5/evidence-index.json")
    _check(index == _build_index(root), "Stage 5 evidence index is stale or incomplete")
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
        target = args.root / "evidence/stage5/evidence-index.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(canonical_json(_build_index(args.root)) + "\n", encoding="utf-8")
        print(f"wrote {target}")
        return
    validate(args.root, args.expect_head)
    print(
        "Stage 5 validation passed: ST5-AC-01 through ST5-AC-22 local gates are closed; "
        "ST5-AC-23 through ST5-AC-25 await exact-head governance and merged-main closure."
    )


if __name__ == "__main__":
    main()
