"""Pure size/charge checks and rejection before local database effects."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from featureforge.aws_runtime import (
    DynamoOnlineRuntime,
    RuntimeConflict,
    candidate_item,
    candidate_write_projection,
    scalar_item_size_upper_bound,
)
from featureforge.canonical import digest
from featureforge.model import FeatureDefinition, FeatureValue
from featureforge.online import build_materialization_plan
from tests.test_stage6_runtime_effects import DatabaseBoundary


def plan_for(count: int = 1) -> Any:
    definition = FeatureDefinition(
        "count", 1, "integer", 1000, "transaction_count", 100, "count", 0
    )
    return build_materialization_plan(
        "risk",
        "op",
        (definition,),
        tuple(
            FeatureValue(f"c{i:04d}", "count", 1, 1000, 1000, definition.definition_digest, "g1")
            for i in range(count)
        ),
        source_digest=digest({"source": 1}),
        materialized_at=1001,
    )


def test_projection_accounts_for_names_utf8_canonical_copy_and_transaction_multiplier() -> None:
    plan = plan_for(3)
    sizes = []
    for record in plan.records:
        item = candidate_item(record)
        # Independent deployed-scalar accounting, never JSON wire length.
        total = sum(len(name.encode()) for name in item)
        total += sum(len(value["S"].encode()) for value in item.values() if "S" in value)
        total += 21 * sum("N" in value for value in item.values())
        total += sum("NULL" in value for value in item.values())
        sizes.append(total)
    projection = candidate_write_projection(plan)
    assert projection["item_bytes_upper_bound"] == sum(sizes)
    assert projection["maximum_item_bytes_upper_bound"] == max(sizes)
    assert projection["transaction_write_units_upper_bound"] == 6 + sum(
        2 * ((size + 1023) // 1024) for size in sizes
    )
    assert projection["transaction_condition_write_units_upper_bound"] == 6
    assert projection["projection_digest"] == digest(
        {key: value for key, value in projection.items() if key != "projection_digest"}
    )
    schema = json.loads(Path("contracts/stage6-candidate-write-projection-v1.json").read_text())
    assert set(projection) == set(schema["required"])
    assert projection["contract"] == schema["properties"]["contract"]["const"]


def test_item_sizes_count_utf8_not_character_count() -> None:
    assert scalar_item_size_upper_bound({"தமிழ்": {"S": "தமிழ்"}}) == 30
    assert scalar_item_size_upper_bound({"a": {"NULL": True}}) == 2


def test_owner_conditions_round_at_write_boundaries_not_read_boundaries() -> None:
    enlarged = replace(plan_for(2), operation_id="o" * 1500)
    projection = candidate_write_projection(enlarged)
    assert 1024 < projection["owner_item_bytes"] <= 2048
    assert projection["transaction_condition_write_units_upper_bound"] == 8


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1e126", "1e-131", "1" * 39])
def test_invalid_service_numbers_reject_before_capacity_projection(value: str) -> None:
    with pytest.raises(RuntimeConflict, match="service bounds"):
        scalar_item_size_upper_bound({"value": {"N": value}})


@pytest.mark.parametrize("value", ["-1e125", "1e-130", "0", "9" * 38])
def test_valid_service_numbers_have_conservative_size(value: str) -> None:
    assert scalar_item_size_upper_bound({"n": {"N": value}}) == 22


@pytest.mark.parametrize("attribute", [{"BOOL": True}, {"M": {}}, {"S": "x", "N": "1"}])
def test_unaccounted_scalar_shapes_never_undercount(attribute: Any) -> None:
    with pytest.raises(RuntimeConflict, match="unaccounted"):
        scalar_item_size_upper_bound({"value": attribute})


def test_oversized_later_record_rejects_before_any_owner_or_record_request() -> None:
    plan = plan_for(2)
    record = replace(plan.records[1], feature_set="a" * 210_000)
    record = replace(record, record_digest=digest(record.body()))
    invalid = replace(plan, records=(plan.records[0], record))
    database = DatabaseBoundary()
    runtime = DynamoOnlineRuntime(database, "online", "control")
    with pytest.raises(RuntimeConflict, match="400 KiB"):
        runtime.materialize(invalid)
    assert database.calls == []
    assert database.items == {}


def test_oversized_owner_rejects_before_database_request() -> None:
    invalid = replace(plan_for(), feature_set="a" * 410_000)
    database = DatabaseBoundary()
    with pytest.raises(RuntimeConflict, match="owner exceeds"):
        DynamoOnlineRuntime(database, "online", "control").materialize(invalid)
    assert database.calls == []


@pytest.mark.parametrize("field", ["customer_id", "feature_name"])
def test_primary_key_limits_reject_before_any_database_effect(field: str) -> None:
    plan = plan_for()
    record = replace(plan.records[0], **{field: "a" * 2100})
    record = replace(record, record_digest=digest(record.body()))
    invalid = replace(plan, records=(record,))
    database = DatabaseBoundary()
    with pytest.raises(RuntimeConflict, match="primary key"):
        DynamoOnlineRuntime(database, "online", "control").materialize(invalid)
    assert database.calls == []
