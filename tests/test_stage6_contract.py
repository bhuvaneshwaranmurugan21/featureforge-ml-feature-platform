from __future__ import annotations

import ast
import json
from pathlib import Path

from featureforge.aws_runtime import task_complete_request, task_get_request, task_put_request
from featureforge.managed import task_request
from featureforge.online import validate_dynamodb_request

ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE = {f"ST6-AC-{number:02d}" for number in range(1, 25)}


def test_stage6_acceptance_and_traceability_are_complete() -> None:
    requirements = json.loads((ROOT / "docs/stage6/requirements.json").read_text())
    traceability = json.loads((ROOT / "docs/stage6/traceability.json").read_text())
    assert {row["id"] for row in requirements["acceptance_criteria"]} == ACCEPTANCE
    assert {row["acceptance"] for row in traceability["mappings"]} == ACCEPTANCE
    for mapping in traceability["mappings"]:
        assert mapping["evidence"]


def test_stage6_contracts_are_closed_and_versioned() -> None:
    contracts = sorted((ROOT / "contracts").glob("stage6-*.json"))
    assert len(contracts) == 8
    for path in contracts:
        schema = json.loads(path.read_text())
        assert schema["$schema"].endswith("2020-12/schema")
        assert schema["$id"].startswith("urn:featureforge:stage6-")
        assert schema["additionalProperties"] is False
        assert schema["required"]


def test_independent_oracle_imports_no_production_code() -> None:
    tree = ast.parse((ROOT / "tests/stage6_oracle.py").read_text())
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert not any(name == "featureforge" or name.startswith("featureforge.") for name in imports)


def test_stage6_dynamodb_task_shapes_validate_against_botocore() -> None:
    record = task_request("run", "VALIDATE", "a" * 64, {"x": 1})
    validate_dynamodb_request("GetItem", task_get_request("table", "run", "VALIDATE"))
    validate_dynamodb_request("PutItem", task_put_request("table", record))
    validate_dynamodb_request("UpdateItem", task_complete_request("table", record, {"ok": True}))


def test_stage6_claim_boundary_remains_honest() -> None:
    baseline = json.loads((ROOT / "evidence/stage6/baseline.json").read_text())
    claims = json.loads((ROOT / "docs/stage6/claims.json").read_text())
    assert baseline["aws_runtime_executed"] is False
    assert baseline["terraform_applied"] is False
    assert {row["class"] for row in claims["claims"]} == {
        "LOCAL_VERIFIED",
        "PENDING",
        "STATICALLY_VALIDATED",
        "UNCLAIMED",
    }
