#!/usr/bin/env python3
"""Fail-closed structural and digest validator for Part 2 Stage 1 / global Stage 3."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
from typing import Any

from featureforge.canonical import digest

PROJECT = "featureforge-ml-feature-platform"
BASE = "d7fa3b4a2d529dca4e8ee03b8eaf58637b583a12"
BASE_TREE = "383e56b3ddbe0f1c4ab6faa7e72270c50684f306"
ACCEPTANCE = {f"ST3-AC-{number:02d}" for number in range(1, 23)}
INDEXED = (
    ".github/workflows/ci.yml",
    "Makefile",
    "README.md",
    "contracts/affected-scope-plan-v1.json",
    "contracts/spark-offline-generation-v1.json",
    "docs/architecture.md",
    "docs/claims.yaml",
    "docs/runbook.md",
    "docs/stage3/claims.json",
    "docs/stage3/failure-repair-log.md",
    "docs/stage3/predecessor.json",
    "docs/stage3/requirements.json",
    "docs/stage3/spark-incremental-specification.md",
    "docs/stage3/traceability.json",
    "docs/stage3/walkthrough.md",
    "evidence/stage3/baseline.json",
    "evidence/stage3/failure-recovery-proof.json",
    "evidence/stage3/spark-proof.json",
    "jobs/spark_point_in_time.py",
    "pyproject.toml",
    "requirements-dev.lock",
    "src/featureforge/spark_runtime.py",
    "tests/stage3_oracle.py",
    "tests/test_stage3_spark.py",
    "tools/run_stage3_proof.py",
    "tools/validate_stage3.py",
)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Stage 3 validation failed: {message}")


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
    source = (root / "tests/stage3_oracle.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden = {"featureforge", "pyspark", "tests.temporal_oracle", "tests.stage2_oracle"}
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    _check(
        not any(name.split(".")[0] in forbidden or name in forbidden for name in imports),
        "Stage 3 oracle imports production or predecessor-oracle logic",
    )


def validate(root: Path) -> None:
    requirements = _load(root, "docs/stage3/requirements.json")
    ids = {row["id"] for row in requirements.get("acceptance_criteria", [])}
    _check(ids == ACCEPTANCE, "requirements must contain exactly ST3-AC-01 through ST3-AC-22")
    traceability = _load(root, "docs/stage3/traceability.json")
    mapped = {row["acceptance"] for row in traceability.get("mappings", [])}
    _check(
        mapped == ACCEPTANCE, "traceability must map every and only Stage 3 acceptance criterion"
    )

    predecessor = _load(root, "docs/stage3/predecessor.json")
    _check(predecessor.get("project") == PROJECT, "predecessor project mismatch")
    _check(predecessor.get("authorized_base_commit") == BASE, "authorized base mismatch")
    _check(predecessor.get("authorized_base_tree") == BASE_TREE, "authorized tree mismatch")
    _check(predecessor.get("part1_status") == "verified-complete", "Part 1 is not verified")

    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    lock = (root / "requirements-dev.lock").read_text(encoding="utf-8")
    for pin in ("pyspark==3.5.9", "py4j==0.10.9.9"):
        _check(pin in pyproject, f"project dependency missing {pin}")
        _check(pin in lock, f"development lock missing {pin}")
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    _check('java-version: "17"' in workflow, "CI does not explicitly select Java 17")
    _check(".[dev,spark]" in workflow, "CI does not install the Spark extra")
    _check("validate_stage3.py" in workflow, "CI does not run the Stage 3 validator")
    _check("run_stage3_proof" in workflow, "CI does not regenerate Stage 3 proofs")

    proof = _load(root, "evidence/stage3/spark-proof.json")
    recovery = _load(root, "evidence/stage3/failure-recovery-proof.json")
    _proof_digest(proof)
    _proof_digest(recovery)
    _check(proof.get("project") == PROJECT, "Spark proof project mismatch")
    _check(proof.get("contract") == "featureforge-stage3-spark-proof-v1", "Spark proof contract")
    runtime = proof.get("runtime", {})
    _check(runtime.get("java_major") == 17, "proof did not use Java 17")
    _check(runtime.get("pyspark") == "3.5.9", "proof did not use pinned PySpark")
    _check(runtime.get("py4j") == "0.10.9.9", "proof did not use pinned Py4J")
    _check(runtime.get("master") == "local[2]", "proof did not use qualified local Spark")
    _check(runtime.get("timezone") == "UTC", "Spark proof timezone is not UTC")
    _check(runtime.get("adaptive_execution") == "false", "proof AQE must be deterministic")

    checks = proof.get("checks", {})
    _check(checks and all(checks.values()), "one or more deterministic Spark checks failed")
    frontiers = proof.get("frontiers", [])
    _check(
        [row.get("knowledge_cutoff") for row in frontiers] == [180000, 200005, 200020],
        "frontier sequence mismatch",
    )
    for frontier in frontiers:
        _check(
            frontier.get("row_count") == 15,
            "frontier does not contain five features x three customers",
        )
        _check(frontier.get("rows_digest") == frontier.get("oracle_digest"), "oracle divergence")
        diagnostics = frontier.get("diagnostics", {})
        _check(not diagnostics.get("cartesian_product"), "Cartesian product detected")
        _check(diagnostics.get("output_rows") == 15, "diagnostic row count mismatch")
        _check(bool(diagnostics.get("physical_plan_digest")), "physical plan digest missing")

    incremental = proof.get("incremental", {})
    _check(incremental.get("equivalent_to_full") is True, "incremental/full equality failed")
    _check(
        incremental.get("rows_digest") == incremental.get("full_rows_digest"),
        "incremental and full digests differ",
    )
    work = incremental.get("work", {})
    _check(work.get("affected_pair_count") == 5, "affected-pair count mismatch")
    _check(work.get("preserved_pair_count") == 10, "preserved-pair count mismatch")
    _check(work.get("work_avoided_pair_count") == 10, "bounded work-avoided count mismatch")
    _check(work.get("recomputed_pair_count") == 5, "recomputed-pair count mismatch")

    workloads = proof.get("workloads", [])
    _check(
        [row.get("profile") for row in workloads] == ["balanced", "skewed", "high-cardinality"],
        "seeded workload profiles mismatch",
    )
    _check(
        [row.get("event_count") for row in workloads] == [48, 120, 160],
        "seeded workload size sequence mismatch",
    )
    _check({row.get("seed") for row in workloads} == {73}, "workload seed mismatch")
    _check(
        all(not row["diagnostics"]["cartesian_product"] for row in workloads),
        "workload diagnostic contains Cartesian product",
    )

    _check(recovery.get("project") == PROJECT, "recovery proof project mismatch")
    _check(recovery.get("written") == "written", "generation was not initially written")
    _check(recovery.get("idempotent_replay") == "replayed", "exact replay was not idempotent")
    _check(
        recovery.get("restart_rows_digest") == recovery.get("expected_rows_digest"),
        "restart did not preserve row identity",
    )
    for field in (
        "partial_generation_rejected",
        "tamper_rejected",
        "corrupt_predecessor_rejected",
        "full_rebuild_fallback",
    ):
        _check(recovery.get(field) is True, f"recovery control failed: {field}")

    _validate_oracle_independence(root)
    contract_ids = {
        "contracts/spark-offline-generation-v1.json": (
            "urn:featureforge:spark-offline-generation:v1"
        ),
        "contracts/affected-scope-plan-v1.json": "urn:featureforge:affected-scope-plan:v1",
    }
    for name, identity in contract_ids.items():
        _check(_load(root, name).get("$id") == identity, f"contract identity mismatch: {name}")

    claims = _load(root, "docs/stage3/claims.json")
    _check(len(claims.get("explicit_non_claims", [])) >= 3, "Stage 3 non-claims are incomplete")
    contamination_files = (
        "src/featureforge/spark_runtime.py",
        "tests/stage3_oracle.py",
        "tests/test_stage3_spark.py",
        "docs/stage3/spark-incremental-specification.md",
        "evidence/stage3/spark-proof.json",
    )
    forbidden_names = ("ledgerguard", "changebridge", "eventpulse", "atlas retail")
    rendered = "\n".join(
        (root / name).read_text(encoding="utf-8").lower() for name in contamination_files
    )
    _check(
        not any(name in rendered for name in forbidden_names),
        "other-project contamination detected",
    )

    baseline = _load(root, "evidence/stage3/baseline.json")
    _check(baseline.get("authorized_base_commit") == BASE, "baseline base mismatch")
    _check(baseline.get("authorized_base_tree") == BASE_TREE, "baseline tree mismatch")
    _check(baseline.get("predecessor_gates_passed") is True, "predecessor gates did not pass")

    index = _load(root, "evidence/stage3/evidence-index.json")
    expected_index = _build_index(root)
    _check(index == expected_index, "Stage 3 evidence index is stale or incomplete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write-index", action="store_true")
    args = parser.parse_args()
    if args.write_index:
        index = _build_index(args.root)
        target = args.root / "evidence/stage3/evidence-index.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {target}")
        return
    validate(args.root)
    print(
        "Stage 3 validation passed: ST3-AC-01 through ST3-AC-21 local gates are closed; "
        "ST3-AC-22 is ready for exact-head CI, review, merge, and independent receipt closure."
    )


if __name__ == "__main__":
    main()
