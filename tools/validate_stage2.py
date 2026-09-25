"""Fail-closed structural and digest validator for Part 1 Stage 2 evidence."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

PROJECT = "featureforge-ml-feature-platform"
BASE = "0f25178f5b163cd121717018a1f7c405a9f80499"
BASE_TREE = "370f0bd81acd95932e5c2d4226104dee8816b834"
REQUIREMENTS = {"FF-P1-06", "FF-P1-07", "FF-P1-08", "FF-P1-09", "FF-P1-10"}
CONTRACT_IDS = {
    "contracts/source-snapshot-manifest-v1.json": "urn:featureforge:source-snapshot-manifest:v1",
    "contracts/training-request-v1.json": "urn:featureforge:training-request:v1",
    "contracts/definition-set-manifest-v1.json": "urn:featureforge:definition-set-manifest:v1",
    "contracts/training-dataset-manifest-v2.json": "urn:featureforge:training-dataset-manifest:v2",
    "contracts/generation-manifest-v1.json": "urn:featureforge:generation-manifest:v1",
    "contracts/validation-receipt-v1.json": "urn:featureforge:validation-receipt:v1",
    "contracts/publication-receipt-v1.json": "urn:featureforge:publication-receipt:v1",
}
INDEXED = (
    ".github/workflows/ci.yml",
    "Makefile",
    "README.md",
    *CONTRACT_IDS,
    "docs/architecture.md",
    "docs/claims.yaml",
    "docs/failure-lab.md",
    "docs/runbook.md",
    "docs/stage2/claims.json",
    "docs/stage2/failure-repair-log.md",
    "docs/stage2/lifecycle-specification.md",
    "docs/stage2/predecessor.json",
    "docs/stage2/requirements.json",
    "docs/stage2/state-transition-table.json",
    "docs/stage2/traceability.json",
    "docs/stage2/walkthrough.md",
    "evidence/stage2/baseline.json",
    "evidence/stage2/failure-recovery-proof.json",
    "evidence/stage2/vertical-slice-proof.json",
    "src/featureforge/lifecycle.py",
    "src/featureforge/provenance.py",
    "tests/fixtures/stage2-lifecycle.json",
    "tests/stage2_oracle.py",
    "tests/stage2_support.py",
    "tests/test_stage2_contract.py",
    "tests/test_stage2_lifecycle.py",
    "tests/test_stage2_provenance.py",
    "tools/run_stage2_proof.py",
    "tools/validate_stage2.py",
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
        "base_sha": BASE,
        "base_tree": BASE_TREE,
        "files_sha256": files,
        "project": PROJECT,
        "stage": "part1-stage2",
    }
    path = root / "evidence/stage2/evidence-index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _validate_identity(document: dict[str, Any], label: str) -> None:
    _check(document.get("project") == PROJECT, f"{label}: wrong project")
    _check(document.get("base_sha") == BASE, f"{label}: stale or wrong base SHA")
    if "base_tree" in document:
        _check(document.get("base_tree") == BASE_TREE, f"{label}: stale or wrong base tree")


def validate(root: Path, *, check_index: bool = True, expect_head: str | None = None) -> None:
    root = root.resolve()
    requirements = _object(root, "docs/stage2/requirements.json")
    claims = _object(root, "docs/stage2/claims.json")
    traceability = _object(root, "docs/stage2/traceability.json")
    states = _object(root, "docs/stage2/state-transition-table.json")
    predecessor = _object(root, "docs/stage2/predecessor.json")
    baseline = _object(root, "evidence/stage2/baseline.json")
    vertical = _object(root, "evidence/stage2/vertical-slice-proof.json")
    recovery = _object(root, "evidence/stage2/failure-recovery-proof.json")
    for label, document in (
        ("requirements", requirements),
        ("claims", claims),
        ("traceability", traceability),
        ("state table", states),
        ("predecessor", predecessor),
        ("baseline", baseline),
        ("vertical proof", vertical),
        ("recovery proof", recovery),
    ):
        _validate_identity(document, label)

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
            _check(isinstance(paths, list) and paths, f"{row['id']}: missing {field}")
            for path in paths:
                _file(root, path)
        controls = row.get("negative_controls")
        _check(isinstance(controls, list) and controls, f"{row['id']}: controls missing")

    links = traceability.get("links")
    _check(isinstance(links, dict) and set(links) == REQUIREMENTS, "traceability incomplete")
    for link in links.values():
        for field in ("fixture", "oracle", "proof"):
            _file(root, link[field])
        for field in ("contracts", "production"):
            paths = link.get(field)
            _check(isinstance(paths, list) and paths, f"traceability {field} missing")
            for path in paths:
                _file(root, path)

    transitions = states.get("transitions")
    _check(isinstance(transitions, list) and len(transitions) == 8, "state table incomplete")
    state_edges = {(row.get("from"), row.get("to")) for row in transitions}
    _check(
        {("VALIDATING", "READY"), ("VALIDATING", "FAILED"), ("READY", "ACTIVE")} <= state_edges,
        "critical state transition missing",
    )

    claim_rows = claims.get("claims")
    _check(isinstance(claim_rows, list) and claim_rows, "claim registry missing")
    for claim in claim_rows:
        _check(claim.get("label") in {"LOCAL_VERIFIED", "UNCLAIMED"}, "unsupported claim")
        _check(bool(claim.get("limitations")), f"{claim.get('id')}: limitations missing")
        for path in claim.get("evidence", []):
            _file(root, path)
    managed = next(claim for claim in claim_rows if claim["id"] == "FF-ST2-CLAIM-04")
    _check(managed["label"] == "UNCLAIMED", "managed runtime is overclaimed")

    _check(vertical.get("result") == "PASS", "vertical proof did not pass")
    _check(recovery.get("result") == "PASS", "recovery proof did not pass")
    _check(vertical.get("dataset", {}).get("oracle_agreement") is True, "oracle mismatch")
    _check(
        vertical.get("dataset", {}).get("historical_unchanged_after_later_corrections") is True,
        "historical artifact changed",
    )
    current_views = vertical.get("current_views", {})
    _check(
        [
            current_views[str(value)]["c1_transaction_count_24h"]
            for value in (180000, 200005, 200020)
        ]
        == [3, 4, 3],
        "correction/retraction current views are wrong",
    )
    fault_points = recovery.get("fault_points")
    _check(isinstance(fault_points, list), "fault matrix missing")
    _check([row.get("id") for row in fault_points] == list(range(1, 21)), "fault matrix incomplete")
    _check(all(row.get("result") == "PASS" for row in fault_points), "fault control failed")
    _check(
        sorted(recovery.get("publication", {}).get("race_results", {}).values())
        == ["committed", "semantic-cas-conflict"],
        "two-publisher semantic CAS proof failed",
    )
    _check(
        recovery.get("final", {}).get("pointer", {}).get("version") == 4,
        "pointer history is incomplete",
    )

    proof_digests = vertical.get("digests", {})
    _check(
        proof_digests.get("fixture_sha256")
        == _digest(_file(root, "tests/fixtures/stage2-lifecycle.json")),
        "stale Stage 2 fixture digest",
    )
    _check(
        proof_digests.get("oracle_sha256") == _digest(_file(root, "tests/stage2_oracle.py")),
        "stale Stage 2 oracle digest",
    )
    code_identity = vertical.get("code_identity", {})
    code_files = code_identity.get("files_sha256")
    _check(isinstance(code_files, dict) and code_files, "code identity file set missing")
    for name, recorded_digest in code_files.items():
        _check(recorded_digest == _digest(_file(root, name)), f"stale code identity: {name}")
    rendered_code_files = json.dumps(
        code_files, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    _check(
        code_identity.get("digest")
        == hashlib.sha256(rendered_code_files.encode("utf-8")).hexdigest(),
        "code identity digest is inconsistent",
    )
    oracle_tree = ast.parse(_file(root, "tests/stage2_oracle.py").read_text(encoding="utf-8"))
    production_imports = [
        node.module
        for node in ast.walk(oracle_tree)
        if isinstance(node, ast.ImportFrom)
        and node.module is not None
        and node.module.startswith("featureforge")
    ]
    _check(production_imports == [], "independent oracle imports production code")

    for name, expected_id in CONTRACT_IDS.items():
        contract = _object(root, name)
        _check(contract.get("$id") == expected_id, f"wrong contract identity: {name}")
        _check(contract.get("additionalProperties") is False, f"contract is not strict: {name}")

    dependency = baseline.get("dependency_probe", {})
    _check(dependency.get("result") == "not_needed", "unexpected dependency gate result")

    if check_index:
        index = _object(root, "evidence/stage2/evidence-index.json")
        _validate_identity(index, "evidence index")
        recorded = index.get("files_sha256")
        _check(
            isinstance(recorded, dict) and set(recorded) == set(INDEXED), "index file set mismatch"
        )
        for name in INDEXED:
            _check(recorded[name] == _digest(_file(root, name)), f"stale evidence digest: {name}")

    if expect_head is not None:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        _check(actual == expect_head, "exact head changed since Stage 2 evidence review")


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
    print("FeatureForge Part 1 Stage 2 contract: PASS")


if __name__ == "__main__":
    main()
