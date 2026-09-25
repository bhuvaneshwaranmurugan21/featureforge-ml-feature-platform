"""Fail-closed structural and digest validator for Part 1 Stage 1 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

PROJECT = "featureforge-ml-feature-platform"
BASE = "ce2646754065145cbf609f286c588bbaa4fb3284"
REQUIREMENTS = {"FF-P1-03", "FF-P1-04", "FF-P1-05"}
INDEXED = (
    ".github/workflows/ci.yml",
    "README.md",
    "contracts/payment-event-v2.json",
    "docs/stage1/claims.json",
    "docs/stage1/decision-table.json",
    "docs/stage1/failure-repair-log.md",
    "docs/stage1/requirements.json",
    "docs/stage1/temporal-specification.md",
    "docs/stage1/traceability.json",
    "evidence/stage1/baseline.json",
    "evidence/stage1/local-simulation.json",
    "evidence/stage1/temporal-proof.json",
    "pyproject.toml",
    "requirements-dev.lock",
    "src/featureforge/computation.py",
    "src/featureforge/dataset.py",
    "src/featureforge/model.py",
    "src/featureforge/store.py",
    "src/featureforge/temporal.py",
    "tests/fixtures/stage1-temporal-cases.json",
    "tests/temporal_oracle.py",
    "tests/test_stage1_contract.py",
    "tests/test_stage1_store_contract.py",
    "tests/test_stage1_temporal_golden.py",
    "tests/test_stage1_temporal_properties.py",
    "tools/run_stage1_proof.py",
    "tools/validate_stage1.py",
)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _file(root: Path, name: str) -> Path:
    _check(bool(name) and not name.startswith("/"), f"unsafe file path: {name}")
    path = (root / name).resolve()
    _check(path.is_relative_to(root.resolve()), f"path escapes repository: {name}")
    _check(path.is_file(), f"missing evidence file: {name}")
    return path


def _object(root: Path, name: str) -> dict[str, Any]:
    value = json.loads(_file(root, name).read_text(encoding="utf-8"))
    _check(isinstance(value, dict), f"{name}: expected JSON object")
    return value


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_index(root: Path) -> None:
    files = {name: _digest(_file(root, name)) for name in INDEXED}
    output = {
        "project": PROJECT,
        "stage": "part1-stage1",
        "base_sha": BASE,
        "files_sha256": files,
    }
    path = root / "evidence/stage1/evidence-index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate(root: Path, *, check_index: bool = True, expect_head: str | None = None) -> None:
    root = root.resolve()
    requirements = _object(root, "docs/stage1/requirements.json")
    claims = _object(root, "docs/stage1/claims.json")
    traceability = _object(root, "docs/stage1/traceability.json")
    decisions = _object(root, "docs/stage1/decision-table.json")
    baseline = _object(root, "evidence/stage1/baseline.json")
    simulation = _object(root, "evidence/stage1/local-simulation.json")
    proof = _object(root, "evidence/stage1/temporal-proof.json")
    for name, document in (
        ("requirements", requirements),
        ("claims", claims),
        ("traceability", traceability),
        ("decisions", decisions),
        ("baseline", baseline),
        ("proof", proof),
    ):
        _check(document.get("project") == PROJECT, f"{name}: wrong project")
        _check(document.get("base_sha") == BASE, f"{name}: stale or wrong base SHA")

    _check(simulation.get("result") == "PASS", "current local simulation did not pass")
    _check(simulation.get("production_claim") is False, "simulation overclaims production proof")

    rows = requirements.get("requirements")
    _check(isinstance(rows, list), "requirements list missing")
    ids = {row.get("id") for row in rows if isinstance(row, dict)}
    _check(ids == REQUIREMENTS and len(rows) == len(ids), "requirement set is incomplete")
    for row in rows:
        _check(
            row.get("status") == "local_verified_pending_merge",
            f"{row.get('id')}: invalid pre-merge status",
        )
        for field in ("specification", "implementation", "tests", "evidence"):
            paths = row.get(field)
            _check(isinstance(paths, list) and bool(paths), f"{row['id']}: missing {field}")
            for path in paths:
                _file(root, path)
        controls = row.get("negative_controls")
        _check(isinstance(controls, list) and bool(controls), f"{row['id']}: controls missing")

    claim_rows = claims.get("claims")
    _check(isinstance(claim_rows, list) and bool(claim_rows), "claim registry missing")
    for claim in claim_rows:
        _check(
            claim.get("label") in {"LOCAL_VERIFIED", "UNCLAIMED"},
            "unsupported claim label",
        )
        _check(bool(claim.get("limitations")), f"{claim.get('id')}: limitation missing")
        for path in claim.get("evidence", []):
            _file(root, path)

    links = traceability.get("links")
    _check(isinstance(links, dict) and set(links) == REQUIREMENTS, "traceability is incomplete")
    for link in links.values():
        for key in ("contract", "fixture", "oracle", "proof"):
            _file(root, link[key])
        for path in link["production"]:
            _file(root, path)

    expected_decisions = {
        "event_on_upper_boundary",
        "event_on_lower_window_boundary",
        "revision_on_knowledge_cutoff",
        "revision_after_knowledge_cutoff",
        "exact_revision_replay",
        "revision_id_different_content",
        "same_event_same_knowledge_time",
        "customer_changes_within_event",
        "knowledge_time_before_event_time",
        "retraction_before_cutoff",
        "retraction_after_cutoff",
        "dataset_frontier_before_prediction_cutoff",
    }
    actual_decisions = {row.get("case") for row in decisions.get("decisions", [])}
    _check(actual_decisions == expected_decisions, "temporal decision table is incomplete")

    _check(
        baseline.get("dependency_probe", {}).get("result") == "passed",
        "dependency probe failed",
    )
    _check(proof.get("result") == "PASS", "temporal proof did not pass")
    controls = proof.get("negative_controls", {})
    expected_controls = {
        "customer_identity_change",
        "invalid_knowledge_clock",
        "leaky_event_time_only_detected",
        "payload_bearing_retraction",
        "revision_id_content_conflict",
        "same_event_same_knowledge_time",
    }
    _check(
        isinstance(controls, dict)
        and set(controls) == expected_controls
        and all(controls.values()),
        "negative control did not fire",
    )
    properties = proof.get("properties", {})
    _check(properties.get("exhaustive_permutations") == 5040, "permutation proof incomplete")
    _check(properties.get("bounded_single_event_cases") == 36, "bounded proof incomplete")
    _check(
        properties.get("hypothesis_derandomized_examples") == 350,
        "property proof incomplete",
    )
    oracle = proof.get("oracle_independence", {})
    _check(oracle.get("production_imports_in_oracle") == [], "oracle imports production logic")
    proof_digests = proof.get("digests", {})
    expected_proof_digests = {
        "contract_v2": "contracts/payment-event-v2.json",
        "decision_table": "docs/stage1/decision-table.json",
        "fixture": "tests/fixtures/stage1-temporal-cases.json",
        "specification": "docs/stage1/temporal-specification.md",
    }
    for key, name in expected_proof_digests.items():
        _check(
            proof_digests.get(key) == _digest(_file(root, name)),
            f"stale proof digest: {name}",
        )
    _check(
        oracle.get("oracle_sha256") == _digest(_file(root, "tests/temporal_oracle.py")),
        "stale oracle digest",
    )
    _check(
        properties.get("hypothesis_source_sha256")
        == _digest(_file(root, "tests/test_stage1_temporal_properties.py")),
        "stale property-test digest",
    )

    contract = _object(root, "contracts/payment-event-v2.json")
    _check(contract.get("$id") == "urn:featureforge:payment-event:v2", "wrong v2 contract")
    _file(root, "contracts/payment-event-v1.json")

    if check_index:
        index = _object(root, "evidence/stage1/evidence-index.json")
        _check(index.get("project") == PROJECT, "index: wrong project")
        _check(index.get("base_sha") == BASE, "index: stale or wrong base SHA")
        recorded = index.get("files_sha256")
        _check(
            isinstance(recorded, dict) and set(recorded) == set(INDEXED),
            "index file set mismatch",
        )
        for name in INDEXED:
            _check(recorded[name] == _digest(_file(root, name)), f"stale evidence digest: {name}")

    if expect_head is not None:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        _check(actual == expect_head, "exact head changed since Stage 1 evidence review")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write-index", action="store_true")
    parser.add_argument("--expect-head")
    args = parser.parse_args()
    if args.write_index:
        validate(args.root, check_index=False)
        write_index(args.root)
    validate(args.root, expect_head=args.expect_head)
    print("FeatureForge Part 1 Stage 1 contract: PASS")


if __name__ == "__main__":
    main()
