from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from featureforge.assurance import (
    ActivationGateInputs,
    ActivationRejected,
    AssuranceError,
    BenchmarkTrial,
    TelemetryEvent,
    activate_eligible,
    calculate_lags,
    evaluate_activation,
    summarize_trials,
    validate_telemetry,
)
from featureforge.computation import materialize
from featureforge.online import ActivationConflict, LocalOnlineStore, build_materialization_plan
from featureforge.provenance import decode_payment_events
from tests.stage2_support import definitions, fixture

FEATURE_SET = "payment-risk-v1"
SOURCE_DIGEST = "a" * 64


def _plan(generation_id: str, operation_id: str, cutoff: int):
    data = fixture()
    defs = definitions(data)
    values = materialize(
        generation_id,
        defs,
        decode_payment_events(data["events"]),
        ("c-1", "c-2", "c-empty"),
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=cutoff,
    )
    return build_materialization_plan(
        FEATURE_SET,
        operation_id,
        defs,
        values,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )


def _inputs(
    generation_id: str,
    operation_id: str,
    receipt_digest: str,
    expected_generation: str | None,
    expected_version: int,
) -> ActivationGateInputs:
    return ActivationGateInputs(
        feature_set=FEATURE_SET,
        generation_id=generation_id,
        operation_id=operation_id,
        validation_receipt_digest=receipt_digest,
        expected_generation=expected_generation,
        expected_version=expected_version,
        evaluated_at=200040,
        evidence_digests=("b" * 64, "c" * 64),
        source_complete=True,
        manifests_valid=True,
        offline_valid=True,
        online_reconciled=True,
        parity_passed=True,
        freshness_passed=True,
        candidate_consistent=True,
        unique_candidate=True,
        pointer_current=True,
    )


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("source_complete", "SOURCE_INCOMPLETE"),
        ("manifests_valid", "MANIFEST_INVALID"),
        ("offline_valid", "OFFLINE_INVALID"),
        ("online_reconciled", "ONLINE_RECONCILIATION_FAILED"),
        ("parity_passed", "PARITY_FAILED"),
        ("freshness_passed", "FRESHNESS_FAILED"),
        ("candidate_consistent", "CANDIDATE_INCONSISTENT"),
        ("unique_candidate", "DUPLICATE_OR_CONFLICTING_CANDIDATE"),
        ("pointer_current", "STALE_POINTER"),
    ],
)
def test_every_activation_component_is_mandatory(field: str, reason: str) -> None:
    inputs = _inputs("candidate", "activate-candidate", "d" * 64, "base", 1)
    decision = evaluate_activation(replace(inputs, **{field: False}))
    assert decision.status == "REJECTED"
    assert decision.reason_codes == (reason,)
    assert decision.verified()


def test_rejected_candidate_is_persisted_and_pointer_is_preserved(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    base = _plan("base", "materialize-base", 180000)
    base_receipt = store.materialize(base)
    store.activate(
        base.generation_id,
        validation_receipt_digest=base_receipt["receipt_digest"],
        expected_generation=None,
        expected_version=0,
        operation_id="activate-base",
    )
    candidate = _plan("candidate", "materialize-candidate", 200020)
    receipt = store.materialize(candidate)
    rejected = evaluate_activation(
        replace(
            _inputs("candidate", "activate-candidate", receipt["receipt_digest"], "base", 1),
            parity_passed=False,
        )
    )
    store.record_activation_decision(rejected.as_dict())
    with pytest.raises(ActivationRejected, match="PARITY_FAILED"):
        activate_eligible(store, rejected, actor="test", reason="must-not-activate")
    assert store.pointer(FEATURE_SET) == ("base", 1)
    assert store.activation_decision("activate-candidate") == rejected.as_dict()


def test_eligible_decision_is_bound_to_activation_and_replay(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    candidate = _plan("candidate", "materialize-candidate", 200020)
    receipt = store.materialize(candidate)
    decision = evaluate_activation(
        _inputs("candidate", "activate-candidate", receipt["receipt_digest"], None, 0)
    )
    first = activate_eligible(store, decision, actor="stage5", reason="all-gates-passed")
    second = activate_eligible(store, decision, actor="stage5", reason="all-gates-passed")
    assert first == second
    assert first["policy_decision_digest"] == decision.decision_digest
    assert store.pointer(FEATURE_SET) == ("candidate", 1)
    assert store.activation_decision("activate-candidate") == decision.as_dict()


def test_stale_eligible_decision_loses_existing_pointer_cas(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    first_plan = _plan("first", "materialize-first", 180000)
    first_receipt = store.materialize(first_plan)
    first_decision = evaluate_activation(
        _inputs("first", "activate-first", first_receipt["receipt_digest"], None, 0)
    )
    second_plan = _plan("second", "materialize-second", 200020)
    second_receipt = store.materialize(second_plan)
    stale = evaluate_activation(
        _inputs("second", "activate-second", second_receipt["receipt_digest"], None, 0)
    )
    activate_eligible(store, first_decision, actor="stage5", reason="first-wins")
    with pytest.raises(ActivationConflict, match="no longer matches"):
        activate_eligible(store, stale, actor="stage5", reason="stale-loses")
    assert store.pointer(FEATURE_SET) == ("first", 1)


def test_clock_model_keeps_lags_distinct_and_rejects_regression() -> None:
    snapshot = calculate_lags(
        event_time=100,
        knowledge_time=110,
        source_frontier_available_at=115,
        build_started_monotonic=120,
        build_completed_monotonic=130,
        materialization_started_monotonic=131,
        materialization_completed_monotonic=140,
        candidate_validated_at=141,
        activated_at=145,
        request_time=150,
        record_frontier=110,
        expires_at=160,
    )
    assert snapshot.source_arrival_lag == 10
    assert snapshot.build_duration == 10
    assert snapshot.materialization_duration == 9
    assert snapshot.activation_lag == 4
    assert snapshot.online_freshness_age == 40
    assert snapshot.ttl_age == -10
    with pytest.raises(AssuranceError, match="regressed"):
        calculate_lags(
            event_time=100,
            knowledge_time=99,
            source_frontier_available_at=115,
            build_started_monotonic=120,
            build_completed_monotonic=130,
            materialization_started_monotonic=131,
            materialization_completed_monotonic=140,
            candidate_validated_at=141,
            activated_at=145,
            request_time=150,
            record_frontier=110,
            expires_at=160,
        )


def test_telemetry_is_bounded_correlated_and_non_sensitive() -> None:
    event = TelemetryEvent(
        event="candidate.activation.rejected",
        occurred_at=200040,
        labels={"phase": "parity", "reason_code": "PARITY_FAILED", "status": "rejected"},
        trace={
            "definition_set_digest": "a" * 64,
            "generation_id": "candidate",
            "operation_id": "activate-candidate",
            "source_frontier_digest": "b" * 64,
        },
        measurements={"mismatch_count": 1},
    )
    validate_telemetry(event)
    assert len(event.as_dict()["event_digest"]) == 64
    with pytest.raises(AssuranceError, match="forbidden"):
        validate_telemetry(replace(event, labels={"customer_id": "c-1"}))
    with pytest.raises(AssuranceError, match="incomplete"):
        validate_telemetry(replace(event, trace={"generation_id": "candidate"}))


def test_benchmark_summary_requires_cold_and_five_correct_warm_trials() -> None:
    rows = [
        BenchmarkTrial(
            case_id="small-balanced",
            operation="parity",
            profile="balanced",
            size_class="small",
            trial=number,
            warm=number > 0,
            duration_ns=100 + number,
            input_digest="a" * 64,
            output_digest="b" * 64,
            correctness_passed=True,
        )
        for number in range(6)
    ]
    summary = summarize_trials(rows)
    assert summary["summaries"][0]["warm_trial_count"] == 5
    assert summary["summaries"][0]["cold_ns"] == [100]
    assert len(summary["summary_digest"]) == 64
    with pytest.raises(AssuranceError, match="incorrect"):
        summarize_trials([replace(rows[0], correctness_passed=False)])
