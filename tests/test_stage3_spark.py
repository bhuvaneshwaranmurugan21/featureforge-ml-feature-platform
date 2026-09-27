from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest

from featureforge.canonical import digest
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.spark_runtime import (
    GenerationArtifactError,
    SparkBuild,
    SparkContractError,
    create_local_spark,
    full_rebuild,
    generate_workload,
    incremental_rebuild,
    plan_affected_scope,
    read_generation,
    write_generation,
)
from tests.stage3_oracle import oracle_rows

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark() -> Any:
    session = create_local_spark("featureforge-stage3-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def stage3_data() -> dict[str, Any]:
    value: dict[str, Any] = json.loads(
        (ROOT / "tests/fixtures/stage2-lifecycle.json").read_text(encoding="utf-8")
    )
    return value


def _definitions(data: dict[str, Any]) -> tuple[FeatureDefinition, ...]:
    return tuple(FeatureDefinition(**row) for row in data["definitions"])


def _events(data: dict[str, Any]) -> tuple[PaymentEvent, ...]:
    return tuple(PaymentEvent(**row) for row in data["events"])


def _oracle_definitions(definitions: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in definitions
    ]


def _canonical(build: Any) -> tuple[dict[str, Any], ...]:
    return tuple(value.as_dict() for value in build.values)


@pytest.mark.parametrize("knowledge_cutoff", [180000, 200005, 200020])
def test_spark_matches_independent_oracle_at_all_frontiers(
    spark: Any, stage3_data: dict[str, Any], knowledge_cutoff: int
) -> None:
    definitions = _definitions(stage3_data)
    build = full_rebuild(
        spark,
        f"full-{knowledge_cutoff}",
        definitions,
        _events(stage3_data),
        ("c-1", "c-2", "c-empty"),
        event_cutoff=stage3_data["event_cutoff"],
        knowledge_cutoff=knowledge_cutoff,
    )
    expected = oracle_rows(
        _oracle_definitions(definitions),
        stage3_data["events"],
        ("c-1", "c-2", "c-empty"),
        f"full-{knowledge_cutoff}",
        event_cutoff=stage3_data["event_cutoff"],
        knowledge_cutoff=knowledge_cutoff,
    )
    assert _canonical(build) == expected
    assert len(build.values) == 15
    assert not build.diagnostics.cartesian_product


def test_business_window_is_open_lower_and_closed_upper(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    build = full_rebuild(
        spark,
        "boundary-proof",
        _definitions(stage3_data),
        _events(stage3_data),
        ("c-1",),
        event_cutoff=200000,
        knowledge_cutoff=200005,
    )
    row = next(value for value in build.values if value.feature_name == "transaction_count_24h")
    assert row.value == 4


def test_future_knowledge_cannot_leak(spark: Any, stage3_data: dict[str, Any]) -> None:
    events = list(_events(stage3_data))
    events.append(
        PaymentEvent("future", "c-1", 199999, 250000, 999999, "succeeded", 1.0, "future-r1")
    )
    base = full_rebuild(
        spark,
        "leak-base",
        _definitions(stage3_data),
        _events(stage3_data),
        ("c-1",),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    candidate = full_rebuild(
        spark,
        "leak-candidate",
        _definitions(stage3_data),
        events,
        ("c-1",),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    assert [value.value for value in base.values] == [value.value for value in candidate.values]


def test_order_and_partition_configuration_do_not_change_rows(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    definitions = _definitions(stage3_data)
    events = _events(stage3_data)
    forward = full_rebuild(
        spark,
        "stable",
        definitions,
        events,
        ("c-1", "c-2", "c-empty"),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    spark.conf.set("spark.sql.shuffle.partitions", "7")
    reverse = full_rebuild(
        spark,
        "stable",
        tuple(reversed(definitions)),
        tuple(reversed(events)),
        ("c-empty", "c-2", "c-1"),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    assert _canonical(forward) == _canonical(reverse)


def test_incremental_equals_full_and_preserved_rows_are_reenveloped(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    definitions = _definitions(stage3_data)
    events = _events(stage3_data)
    customers = ("c-1", "c-2", "c-empty")
    base = full_rebuild(
        spark,
        "base",
        definitions,
        events,
        customers,
        event_cutoff=200000,
        knowledge_cutoff=180000,
    )
    incremental, plan = incremental_rebuild(
        spark,
        "target",
        definitions,
        events,
        customers,
        base,
        event_cutoff=200000,
        target_knowledge_cutoff=200020,
    )
    full = full_rebuild(
        spark,
        "target",
        definitions,
        events,
        customers,
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    assert plan.mode == "incremental"
    assert _canonical(incremental) == _canonical(full)
    assert all(value.generation_id == "target" for value in incremental.values)
    assert all(value.knowledge_time == 200020 for value in incremental.values)


def test_scope_mutant_that_omits_changed_pair_is_detected(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    definitions = _definitions(stage3_data)
    events = _events(stage3_data)
    base = full_rebuild(
        spark,
        "base-mutant",
        definitions,
        events,
        ("c-1", "c-2"),
        event_cutoff=200000,
        knowledge_cutoff=180000,
    )
    full = full_rebuild(
        spark,
        "target-mutant",
        definitions,
        events,
        ("c-1", "c-2"),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    plan = plan_affected_scope(
        definitions,
        events,
        event_cutoff=200000,
        base_knowledge_cutoff=180000,
        target_knowledge_cutoff=200020,
    )
    base_values = {(value.customer_id, value.feature_name): value.value for value in base.values}
    full_values = {(value.customer_id, value.feature_name): value.value for value in full.values}
    changed = {key for key in base_values if base_values[key] != full_values[key]}
    assert changed
    assert changed <= set(plan.affected_pairs)
    removed = next(iter(changed))
    assert base_values[removed] != full_values[removed]


def test_threshold_forces_safe_full_rebuild(spark: Any, stage3_data: dict[str, Any]) -> None:
    definitions = _definitions(stage3_data)
    events = _events(stage3_data)
    base = full_rebuild(
        spark,
        "base-fallback",
        definitions,
        events,
        ("c-1", "c-2"),
        event_cutoff=200000,
        knowledge_cutoff=180000,
    )
    rebuilt, plan = incremental_rebuild(
        spark,
        "target-fallback",
        definitions,
        events,
        ("c-1", "c-2"),
        base,
        event_cutoff=200000,
        target_knowledge_cutoff=200020,
        maximum_pair_fraction=0.01,
    )
    assert plan.mode == "full"
    assert rebuilt.manifest.get("incremental") is None


def test_event_time_correction_marks_both_old_and_new_window_effects(
    stage3_data: dict[str, Any],
) -> None:
    definitions = _definitions(stage3_data)
    events = (
        PaymentEvent("move-in", "c-1", 100000, 100000, 1, "succeeded", 0.1, "move-r1"),
        PaymentEvent("move-in", "c-1", 199000, 199500, 1, "succeeded", 0.1, "move-r2"),
        PaymentEvent("move-out", "c-2", 199000, 199000, 1, "succeeded", 0.1, "out-r1"),
        PaymentEvent("move-out", "c-2", 100000, 199500, 1, "succeeded", 0.1, "out-r2"),
    )
    plan = plan_affected_scope(
        definitions,
        events,
        event_cutoff=200000,
        base_knowledge_cutoff=199000,
        target_knowledge_cutoff=200020,
    )
    assert ("c-1", "transaction_count_24h") in plan.affected_pairs
    assert ("c-2", "transaction_count_24h") in plan.affected_pairs


def test_integer_features_do_not_lose_precision_in_union(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    amount = 9_007_199_254_740_993
    event = PaymentEvent(
        "large-integer", "c-large", 199999, 199999, amount, "succeeded", 0.5, "large-r1"
    )
    build = full_rebuild(
        spark,
        "integer-precision",
        _definitions(stage3_data),
        (event,),
        ("c-large",),
        event_cutoff=200000,
        knowledge_cutoff=200000,
    )
    spend = next(
        value for value in build.values if value.feature_name == "successful_spend_30d_cents"
    )
    assert spend.value == amount


def test_successful_spend_uses_typed_default_when_no_success_exists(spark: Any) -> None:
    definition = FeatureDefinition(
        "custom_spend",
        1,
        "integer",
        1000,
        "successful_spend_cents",
        1000,
        "Custom empty-default proof.",
        777,
    )
    failed = PaymentEvent("failed-only", "c-default", 9000, 9000, 25, "failed", 0.4, "failed-r1")
    build = full_rebuild(
        spark,
        "typed-default",
        (definition,),
        (failed,),
        ("c-default",),
        event_cutoff=9500,
        knowledge_cutoff=9500,
    )
    assert build.values[0].value == 777


def test_max_risk_uses_typed_default_when_no_event_exists(spark: Any) -> None:
    definition = FeatureDefinition(
        "custom_risk",
        1,
        "float",
        1000,
        "max_merchant_risk",
        1000,
        "Custom empty-default proof.",
        0.25,
    )
    build = full_rebuild(
        spark,
        "risk-default",
        (definition,),
        (),
        ("c-empty",),
        event_cutoff=9500,
        knowledge_cutoff=9500,
    )
    assert build.values[0].value == 0.25


def test_invalid_and_ambiguous_inputs_fail_closed(spark: Any, stage3_data: dict[str, Any]) -> None:
    events = list(_events(stage3_data))
    events.append(replace(events[0], revision_id="competing-revision"))
    with pytest.raises(SparkContractError, match="conflicting revisions"):
        full_rebuild(
            spark,
            "invalid",
            _definitions(stage3_data),
            events,
            ("c-1",),
            event_cutoff=200000,
            knowledge_cutoff=200020,
        )
    wrong_type = FeatureDefinition(
        "invalid_ratio", 1, "integer", 100, "failed_payment_ratio", 100, "Invalid.", 0
    )
    with pytest.raises(SparkContractError, match="requires float"):
        full_rebuild(
            spark,
            "wrong-type",
            (wrong_type,),
            _events(stage3_data),
            ("c-1",),
            event_cutoff=200000,
            knowledge_cutoff=200020,
        )


def test_forged_predecessor_frontier_is_rejected(spark: Any, stage3_data: dict[str, Any]) -> None:
    definitions = _definitions(stage3_data)
    events = _events(stage3_data)
    base = full_rebuild(
        spark,
        "bound-base",
        definitions,
        events,
        ("c-1",),
        event_cutoff=200000,
        knowledge_cutoff=180000,
    )
    forged_values = (replace(base.values[0], knowledge_time=180001), *base.values[1:])
    forged_rows = tuple(value.as_dict() for value in forged_values)
    forged = SparkBuild(
        base.generation_id,
        forged_values,
        base.manifest | {"rows_digest": digest(forged_rows)},
        base.diagnostics,
    )
    with pytest.raises(GenerationArtifactError, match="declared frontier"):
        incremental_rebuild(
            spark,
            "forged-target",
            definitions,
            events,
            ("c-1",),
            forged,
            event_cutoff=200000,
            target_knowledge_cutoff=200020,
        )


def test_immutable_generation_replay_restart_and_tamper_detection(
    spark: Any, stage3_data: dict[str, Any], tmp_path: Path
) -> None:
    build = full_rebuild(
        spark,
        "durable",
        _definitions(stage3_data),
        _events(stage3_data),
        ("c-1", "c-empty"),
        event_cutoff=200000,
        knowledge_cutoff=200020,
    )
    assert write_generation(build, tmp_path) == "written"
    assert write_generation(build, tmp_path) == "replayed"
    reopened = read_generation(tmp_path / "durable")
    assert reopened.values == build.values
    rows_path = tmp_path / "durable" / "rows.json"
    rows_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(GenerationArtifactError, match="rows do not match"):
        read_generation(tmp_path / "durable")


def test_incomplete_generation_is_never_readable(tmp_path: Path) -> None:
    partial = tmp_path / "partial"
    partial.mkdir()
    (partial / "rows.json").write_text("[]\n", encoding="utf-8")
    with pytest.raises(GenerationArtifactError, match="incomplete"):
        read_generation(partial)


def test_generation_identity_cannot_escape_artifact_root(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    with pytest.raises(SparkContractError, match="path-safe"):
        full_rebuild(
            spark,
            "../escape",
            _definitions(stage3_data),
            _events(stage3_data),
            ("c-1",),
            event_cutoff=200000,
            knowledge_cutoff=200020,
        )


def test_local_driver_collection_has_an_explicit_bound(
    spark: Any, stage3_data: dict[str, Any]
) -> None:
    customers = tuple(f"c-{number}" for number in range(20_001))
    with pytest.raises(SparkContractError, match="driver-row limit"):
        full_rebuild(
            spark,
            "bounded-output",
            _definitions(stage3_data),
            (),
            customers,
            event_cutoff=200000,
            knowledge_cutoff=200020,
        )


@pytest.mark.parametrize("profile", ["balanced", "skewed", "high-cardinality"])
def test_seeded_workload_is_deterministic_and_profiled(profile: str) -> None:
    selected = profile  # narrow for the runtime API without weakening the test matrix.
    first = generate_workload(selected, seed=73, customers=20, events=100)  # type: ignore[arg-type]
    second = generate_workload(selected, seed=73, customers=20, events=100)  # type: ignore[arg-type]
    assert first == second
    assert len(first.customer_ids) == 20
    assert len(first.events) == 100
    if profile == "skewed":
        hot = sum(event.customer_id == first.customer_ids[0] for event in first.events)
        assert hot >= 70
