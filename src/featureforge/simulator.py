"""Deterministic bitemporal and serving-parity failure lab."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

from featureforge.canonical import digest
from featureforge.computation import FeatureTypeError, compute, materialize
from featureforge.dataset import build_training_dataset
from featureforge.definitions import payment_features
from featureforge.model import FeatureDefinition, Label, PaymentEvent
from featureforge.parity import compare_parity
from featureforge.registry import DefinitionConflict, FeatureRegistry
from featureforge.store import (
    CompareAndSwapConflict,
    FeatureStore,
    GenerationConflict,
    ReplayConflict,
)


def _expect(error_type: type[Exception], operation: Callable[[], object]) -> bool:
    try:
        operation()
    except error_type:
        return True
    return False


def events_fixture() -> tuple[PaymentEvent, ...]:
    return (
        PaymentEvent("p-1", "c-1", 1_000, 1_010, 1_000, "succeeded", 0.2),
        PaymentEvent("p-2", "c-1", 2_000, 2_010, 500, "failed", 0.7),
        PaymentEvent("p-3", "c-1", 4_000, 4_010, 2_000, "succeeded", 0.1),
        # Late correction: it must not rewrite what was known at label time 3,000.
        PaymentEvent("p-1", "c-1", 1_000, 3_500, 9_000, "succeeded", 0.2),
    )


def run_failure_lab() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def record(name: str, passed: bool, proof: Any) -> None:
        checks.append({"check": name, "passed": passed, "proof": proof})

    definitions = payment_features()
    registry = FeatureRegistry()
    for definition in definitions:
        registry.register(definition)
    drifted = replace(definitions[0], description="silently changed implementation contract")
    record(
        "definition_drift_blocked",
        _expect(DefinitionConflict, lambda: registry.register(drifted)),
        definitions[0].definition_id,
    )

    store = FeatureStore()
    events = events_fixture()
    for event in events:
        store.append_event(event)
    record("event_replay_idempotent", store.append_event(events[0]) == "replayed", "p-1@1010")
    conflicting_event = replace(events[0], amount_cents=8_888)
    record(
        "conflicting_event_revision_blocked",
        _expect(ReplayConflict, lambda: store.append_event(conflicting_event)),
        "same event_id and knowledge_time, different digest",
    )

    label = Label("label-1", "c-1", 3_000, 1)
    dataset = build_training_dataset(
        "g-1", (label,), definitions, store.events(), dataset_as_of=5_000
    )
    row = dataset.rows[0]
    record(
        "future_event_excluded",
        row["transaction_count_24h"] == 2,
        "p-3 event_time=4000 > label_time=3000",
    )
    record(
        "late_correction_excluded",
        row["successful_spend_30d_cents"] == 1_000,
        "p-1 correction knowledge_time=3500 > label_time=3000",
    )
    repeated = build_training_dataset(
        "g-1", (label,), definitions, store.events(), dataset_as_of=5_000
    )
    record(
        "dataset_reproducibility",
        dataset.manifest == repeated.manifest,
        dataset.manifest["manifest_digest"],
    )

    definition_set_digest = digest(
        sorted(definition.definition_digest for definition in definitions)
    )
    store.create_generation(
        "g-1", definition_set_digest, source_frontier=5_000, dataset_as_of=5_000
    )
    values = materialize(
        "g-1",
        definitions,
        store.events(),
        ("c-1", "missing-customer"),
        event_cutoff=5_000,
        knowledge_cutoff=5_000,
    )
    record("offline_materialization", store.put_offline(values) == "written", len(values))
    record("materialization_replay", store.put_offline(values) == "replayed", "identical digest")
    missing = [value for value in values if value.customer_id == "missing-customer"]
    record(
        "missing_entity_defaults",
        any(value.value is None for value in missing)
        and any(value.value == 0 for value in missing),
        [value.as_dict() for value in missing],
    )

    store.stage_online("g-1")
    parity = compare_parity(store.offline_values("g-1"), store.online_values("g-1"))
    record("offline_online_parity", parity.matched, parity.as_dict())
    altered_online = tuple(
        replace(value, value=999) if value.feature_name == "transaction_count_24h" else value
        for value in store.online_values("g-1")
    )
    mismatch = compare_parity(store.offline_values("g-1"), altered_online)
    record("parity_mismatch_detected", not mismatch.matched, mismatch.as_dict())
    record(
        "parity_gate_blocks_publication",
        _expect(GenerationConflict, lambda: store.mark_ready("g-1", parity_matched=False)),
        "generation remains building",
    )
    store.mark_ready("g-1", parity_matched=parity.matched)
    version = store.activate("g-1", expected_version=0)
    record("atomic_online_publication", version == 1, store.active_pointer())
    store.create_generation(
        "g-stale", definition_set_digest, source_frontier=5_000, dataset_as_of=5_000
    )
    stale_values = tuple(replace(value, generation_id="g-stale") for value in values)
    store.put_offline(stale_values)
    store.stage_online("g-stale")
    stale_parity = compare_parity(
        store.offline_values("g-stale"), store.online_values("g-stale")
    )
    store.mark_ready("g-stale", parity_matched=stale_parity.matched)
    record(
        "stale_publication_blocked",
        _expect(CompareAndSwapConflict, lambda: store.activate("g-stale", expected_version=0)),
        "stale pointer version=0",
    )

    served = store.serve(
        "c-1", "transaction_count_24h", request_time=5_001, ttl_seconds=definitions[0].ttl_seconds
    )
    expired = store.serve(
        "c-1",
        "transaction_count_24h",
        request_time=5_000 + definitions[0].ttl_seconds,
        ttl_seconds=definitions[0].ttl_seconds,
    )
    record("online_ttl", served == 3 and expired is None, {"fresh": served, "expired": expired})

    bad_definition = FeatureDefinition(
        "bad_ratio",
        1,
        "integer",
        100,
        "failed_payment_ratio",
        None,
        "type mismatch injection",
        0,
    )
    record(
        "type_contract_enforced",
        _expect(FeatureTypeError, lambda: compute(bad_definition, store.events(), 5_000)),
        "float computation declared integer",
    )

    store.create_generation(
        "g-backfill", definition_set_digest, source_frontier=3_000, dataset_as_of=3_000
    )
    backfill = materialize(
        "g-backfill",
        definitions,
        store.events(),
        ("c-1",),
        event_cutoff=3_000,
        knowledge_cutoff=3_000,
    )
    store.put_offline(backfill)
    record(
        "backfill_generation_isolation",
        store.active_pointer()[0] == "g-1"
        and all(value.generation_id == "g-backfill" for value in backfill),
        "active pointer unchanged",
    )

    store.close()
    return {
        "architecture": "bitemporal-feature-generations",
        "claim_level": "LOCAL_SIMULATION",
        "checks": checks,
        "evidence_digest": digest(checks),
        "metrics": {
            "checks_passed": sum(item["passed"] for item in checks),
            "checks_total": len(checks),
        },
        "production_claim": False,
        "result": "PASS" if all(item["passed"] for item in checks) else "FAIL",
        "scope": "SQLite correctness oracle; no managed AWS feature store was invoked",
    }
