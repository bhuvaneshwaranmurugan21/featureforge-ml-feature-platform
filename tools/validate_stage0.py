"""Fail-closed structural and digest checks for the FeatureForge Stage 0 contract.

This checks evidence structure and exact file bytes. It cannot certify the truth
of a natural-language claim or replace the executed local/CI commands.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

PROJECT = "featureforge-ml-feature-platform"
BASE = "85d2cf08ecfbd75d88192e55a8a6db9a6ad55397"
TREE = "b5c3ba78228801e7e42a8c219235afb2919401b4"
IDS = {f"FF-P1-{number:02d}" for number in range(1, 11)}
LABELS = {"DESIGN_ONLY", "LOCAL_VERIFIED", "AWS_VERIFIED", "MEASURED", "EXTRAPOLATED", "UNCLAIMED"}
INDEXED = (
    ".github/workflows/ci.yml",
    "README.md",
    "docs/claims.yaml",
    "docs/stage0/README.md",
    "docs/stage0/baseline.json",
    "docs/stage0/claims.json",
    "docs/stage0/fixture-decision.md",
    "docs/stage0/inventory.json",
    "docs/stage0/requirements.json",
    "docs/stage0/risk-register.md",
    "requirements-dev.lock",
    "tests/test_stage0_contract.py",
    "tools/validate_stage0.py",
)


def _object(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path.name}: expected JSON object")
    return data


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _file(root: Path, name: str) -> Path:
    _check(bool(name) and not name.startswith("/"), f"unsafe file path: {name}")
    result = (root / name).resolve()
    _check(result.is_relative_to(root.resolve()), f"path escapes repository: {name}")
    _check(result.is_file(), f"missing evidence file: {name}")
    return result


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_index(root: Path) -> None:
    entries = {name: _digest(_file(root, name)) for name in INDEXED}
    path = root / "docs/stage0/evidence-index.json"
    path.write_text(
        json.dumps(
            {"project": PROJECT, "base_sha": BASE, "files_sha256": entries},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def validate(root: Path, *, check_index: bool = True, expect_head: str | None = None) -> None:
    root = root.resolve()
    requirements = _object(_file(root, "docs/stage0/requirements.json"))
    claims = _object(_file(root, "docs/stage0/claims.json"))
    inventory = _object(_file(root, "docs/stage0/inventory.json"))
    baseline = _object(_file(root, "docs/stage0/baseline.json"))
    for name, data in (
        ("requirements", requirements),
        ("claims", claims),
        ("inventory", inventory),
        ("baseline", baseline),
    ):
        _check(data.get("project") == PROJECT, f"{name}: wrong project")
        _check(data.get("base_sha") == BASE, f"{name}: stale or wrong base SHA")
    _check(
        inventory.get("base_tree") == TREE and baseline.get("base_tree") == TREE,
        "base tree mismatch",
    )

    rows = requirements.get("requirements")
    _check(isinstance(rows, list), "requirements list missing")
    ids = [row.get("id") for row in rows if isinstance(row, dict)]
    _check(
        len(ids) == len(rows) and len(ids) == len(set(ids)) and set(ids) == IDS,
        "missing, duplicate, or unexpected Part 1 requirement",
    )
    stages = {row["id"]: row.get("owner_stage") for row in rows}
    for row in rows:
        _check(row.get("owner_stage") in (0, 1, 2), f"{row['id']}: invalid stage")
        for field in ("requirement", "validation", "negative_control"):
            _check(
                isinstance(row.get(field), str) and bool(row[field].strip()),
                f"{row['id']}: missing {field}",
            )
        evidence = row.get("evidence")
        _check(isinstance(evidence, list) and bool(evidence), f"{row['id']}: missing evidence")
        for item in evidence:
            _check(isinstance(item, str) and bool(item), f"{row['id']}: invalid evidence")
            if item == "docs/stage0/evidence-index.json" and not check_index:
                continue  # The index is generated only after the other contract checks.
            if not item.startswith("planned:"):
                _file(root, item)
        deps = row.get("dependencies")
        _check(
            isinstance(deps, list) and len(deps) == len(set(deps)),
            f"{row['id']}: invalid dependencies",
        )
        _check(
            all(
                dep in IDS and dep != row["id"] and stages[dep] <= stages[row["id"]] for dep in deps
            ),
            f"{row['id']}: missing, self, or forward dependency",
        )
        _check(
            row.get("status") in {"observed_locally", "planned", "pending_merge_proof"},
            f"{row['id']}: invalid proof status",
        )
        _check(
            row["owner_stage"] == 0 or row["status"] == "planned",
            f"{row['id']}: future stage cannot be marked verified",
        )

    claim_rows = claims.get("claims")
    _check(isinstance(claim_rows, list) and bool(claim_rows), "claim register missing")
    claim_ids = [row.get("id") for row in claim_rows if isinstance(row, dict)]
    _check(
        len(claim_ids) == len(claim_rows) and len(set(claim_ids)) == len(claim_ids),
        "duplicate or invalid claim ID",
    )
    for row in claim_rows:
        _check(row.get("label") in LABELS, f"{row.get('id')}: invalid claim label")
        _check(
            row["label"] not in {"AWS_VERIFIED", "MEASURED", "EXTRAPOLATED"},
            f"{row['id']}: unsupported Stage 0 runtime/measurement claim",
        )
        _check(
            bool(row.get("statement")) and bool(row.get("limitations")),
            f"{row['id']}: statement or limitation missing",
        )
        evidence = row.get("evidence")
        _check(isinstance(evidence, list) and bool(evidence), f"{row['id']}: evidence missing")
        for item in evidence:
            _check(isinstance(item, str), f"{row['id']}: bad evidence path")
            _file(root, item)

    components = inventory.get("components")
    _check(isinstance(components, list) and len(components) >= 8, "inventory incomplete")
    classifications = {
        "implemented_and_executed_locally",
        "implemented_and_executed_locally_or_ci",
        "implemented_but_runtime_unverified",
        "scaffolded_or_modeled",
        "absent_as_executable_adapter",
    }
    for item in components:
        _check(
            item.get("classification") in classifications and bool(item.get("gap")),
            "inventory classification or gap missing",
        )
        for path in item.get("paths", []) + item.get("evidence", []):
            _file(root, path)
    draft = inventory.get("open_draft_pr", {})
    _check(
        draft.get("number") == 1 and draft.get("status") == "open_unmerged",
        "draft PR overlap not recorded",
    )

    _check(
        baseline.get("simulation_sha256") == _digest(_file(root, "evidence/local-simulation.json")),
        "committed simulation digest mismatch",
    )
    commands = baseline.get("commands")
    _check(isinstance(commands, list) and len(commands) >= 5, "baseline command transcript missing")
    _check(
        all(
            row.get("exit_code") == 0 and row.get("command") and row.get("result")
            for row in commands
        ),
        "baseline has failed or incomplete command",
    )
    _check(
        baseline.get("ci", {}).get("head_sha") == BASE, "baseline CI is not bound to audited main"
    )

    if check_index:
        index = _object(_file(root, "docs/stage0/evidence-index.json"))
        _check(
            index.get("project") == PROJECT and index.get("base_sha") == BASE,
            "evidence index project or base mismatch",
        )
        recorded = index.get("files_sha256")
        _check(
            isinstance(recorded, dict) and set(recorded) == set(INDEXED),
            "evidence index file set mismatch",
        )
        for name in INDEXED:
            _check(recorded[name] == _digest(_file(root, name)), f"stale evidence digest: {name}")

    if expect_head is not None:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        _check(actual == expect_head, "exact head changed since evidence review")


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
    print("FeatureForge Stage 0 contract: PASS")


if __name__ == "__main__":
    main()
