from __future__ import annotations

import ast
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from featureforge.expected import (
    ExpectedStateError,
    compare_online_records,
    deterministic_stratified_sample,
    project_feature_rows,
    project_online_records,
)
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.online import build_materialization_plan
from featureforge.spark_runtime import create_local_spark, full_rebuild
from tests.stage2_support import definitions, fixture

FEATURE_SET = "payment-risk-v1"
SOURCE_DIGEST = "a" * 64
CUSTOMERS = ("c-1", "c-2", "c-empty")


def _definition_rows(defs: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in defs
    ]


def _events(data: dict[str, Any]) -> tuple[PaymentEvent, ...]:
    return tuple(PaymentEvent(**row) for row in data["events"])


@pytest.fixture(scope="module")
def spark() -> Any:
    session = create_local_spark("featureforge-stage5-expected-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_expected_projector_imports_no_production_logic() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "src/featureforge/expected.py"
    ).read_text(encoding="utf-8")
    imports: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert not any(name == "featureforge" or name.startswith("featureforge.") for name in imports)


def test_expected_state_matches_spark_and_online_envelopes(spark: Any) -> None:
    data = fixture()
    defs = definitions(data)
    expected_features = project_feature_rows(
        _definition_rows(defs),
        data["events"],
        CUSTOMERS,
        "stage5-target",
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    actual = full_rebuild(
        spark,
        "stage5-target",
        defs,
        _events(data),
        CUSTOMERS,
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    assert tuple(value.as_dict() for value in actual.values) == expected_features
    expected_online = project_online_records(
        _definition_rows(defs),
        expected_features,
        feature_set=FEATURE_SET,
        operation_id="materialize-stage5",
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    plan = build_materialization_plan(
        FEATURE_SET,
        "materialize-stage5",
        defs,
        actual.values,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    observed = tuple(record.as_dict() for record in plan.records)
    report = compare_online_records(expected_online, observed)
    assert report["passed"] is True
    assert report["reasons"] == []


def test_comparator_detects_membership_semantics_integrity_and_eligibility() -> None:
    data = fixture()
    defs = definitions(data)
    features = project_feature_rows(
        _definition_rows(defs),
        data["events"],
        CUSTOMERS,
        "candidate",
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    expected = list(
        project_online_records(
            _definition_rows(defs),
            features,
            feature_set=FEATURE_SET,
            operation_id="candidate-op",
            source_digest=SOURCE_DIGEST,
            materialized_at=200030,
        )
    )

    missing = compare_online_records(expected, expected[1:])
    assert {"MISSING_RECORD", "AGGREGATE_MISMATCH"} <= set(missing["reasons"])

    extra_row = dict(expected[0])
    extra_row["customer_id"] = "extra"
    extra_row["partition_key"] = "ENTITY#extra#GEN#candidate"
    extra = compare_online_records(expected, [*expected, extra_row])
    assert "EXTRA_RECORD" in extra["reasons"]

    wrong = [dict(row) for row in expected]
    wrong[0]["value"] = {"integer": 999, "kind": "integer"}
    mismatch = compare_online_records(expected, wrong)
    assert "SEMANTIC_MISMATCH" in mismatch["reasons"]
    assert "ENVELOPE_MISMATCH" in mismatch["reasons"]

    stale = compare_online_records(
        expected,
        expected,
        request_time=200031,
        maximum_freshness_age=10,
    )
    assert "INELIGIBLE_RECORD" in stale["reasons"]
    assert {row["reason"] for row in stale["eligibility"]} == {"STALE"}

    expiry = compare_online_records(expected, expected, request_time=9_999_999)
    assert "INELIGIBLE_RECORD" in expiry["reasons"]
    assert {row["reason"] for row in expiry["eligibility"]} == {"EXPIRED"}


def test_stratified_sample_is_stable_and_covers_declared_strata() -> None:
    records = [
        {"customer_id": customer, "feature_name": f"feature-{number}"}
        for customer in ("hot", "sparse", "boundary", "ordinary")
        for number in range(4)
    ]
    strata = {"hot": "hot", "sparse": "sparse", "boundary": "boundary"}
    first = deterministic_stratified_sample(records, strata, seed=73, per_stratum=2)
    second = deterministic_stratified_sample(
        tuple(reversed(records)), strata, seed=73, per_stratum=2
    )
    changed = deterministic_stratified_sample(records, strata, seed=74, per_stratum=2)
    assert first == second
    assert first != changed
    assert {strata.get(customer, "ordinary") for customer, _ in first} == {
        "boundary",
        "hot",
        "ordinary",
        "sparse",
    }
    with pytest.raises(ExpectedStateError, match="positive"):
        deterministic_stratified_sample(records, strata, seed=73, per_stratum=0)


def test_projector_rejects_ambiguous_revision_and_unknown_definition() -> None:
    data = fixture()
    defs = definitions(data)
    conflict = dict(data["events"][0])
    conflict["amount_cents"] = int(conflict["amount_cents"]) + 1
    with pytest.raises(ExpectedStateError, match="revision identity conflict"):
        project_feature_rows(
            _definition_rows(defs),
            [*data["events"], conflict],
            CUSTOMERS,
            "bad",
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=200020,
        )
    rows = list(
        project_feature_rows(
            _definition_rows(defs),
            data["events"],
            CUSTOMERS,
            "bad-definition",
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=200020,
        )
    )
    rows[0] = dict(rows[0]) | {"definition_digest": "0" * 64}
    with pytest.raises(ExpectedStateError, match="unknown definition"):
        project_online_records(
            _definition_rows(defs),
            rows,
            feature_set=FEATURE_SET,
            operation_id="bad",
            source_digest=SOURCE_DIGEST,
            materialized_at=200030,
        )


def test_single_semantic_error_has_generation_integrity_blast_radius() -> None:
    data = fixture()
    defs = definitions(data)
    features = project_feature_rows(
        _definition_rows(defs),
        data["events"],
        CUSTOMERS,
        "blast-radius",
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    expected = project_online_records(
        _definition_rows(defs),
        features,
        feature_set=FEATURE_SET,
        operation_id="blast-radius-op",
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    changed = list(features)
    changed[0] = dict(changed[0]) | {"value": int(changed[0]["value"]) + 1}
    actual = project_online_records(
        _definition_rows(defs),
        changed,
        feature_set=FEATURE_SET,
        operation_id="blast-radius-op",
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    report = compare_online_records(expected, actual)
    assert len(report["semantic_mismatches"]) == 1
    assert report["envelope_mismatch_count"] == len(expected)
