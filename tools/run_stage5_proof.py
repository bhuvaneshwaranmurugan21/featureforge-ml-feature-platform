#!/usr/bin/env python3
"""Generate deterministic Stage 5 parity, policy, and recovery evidence."""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from featureforge.assurance import (
    ActivationGateInputs,
    BenchmarkTrial,
    TelemetryEvent,
    activate_eligible,
    calculate_lags,
    evaluate_activation,
    summarize_trials,
    validate_telemetry,
)
from featureforge.canonical import canonical_json, digest
from featureforge.computation import materialize
from featureforge.expected import (
    compare_online_records,
    deterministic_stratified_sample,
    project_feature_rows,
    project_online_records,
)
from featureforge.model import FeatureDefinition
from featureforge.online import ActivationConflict, LocalOnlineStore, build_materialization_plan
from featureforge.provenance import decode_payment_events
from tests.stage2_support import definitions, fixture

PROJECT = "featureforge-ml-feature-platform"
FEATURE_SET = "payment-risk-v1"
SOURCE_DIGEST = "a" * 64
CUSTOMERS = ("c-1", "c-2", "c-empty")


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def _definition_rows(rows: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [asdict(row) | {"definition_digest": row.definition_digest} for row in rows]


def _gate_inputs(
    generation_id: str,
    operation_id: str,
    receipt_digest: str,
    *,
    expected_generation: str | None,
    expected_version: int,
    evidence_digests: tuple[str, ...],
) -> ActivationGateInputs:
    return ActivationGateInputs(
        feature_set=FEATURE_SET,
        generation_id=generation_id,
        operation_id=operation_id,
        validation_receipt_digest=receipt_digest,
        expected_generation=expected_generation,
        expected_version=expected_version,
        evaluated_at=200040,
        evidence_digests=evidence_digests,
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


def _benchmark_shape() -> dict[str, Any]:
    rows = [
        BenchmarkTrial(
            case_id="proof-small-balanced",
            operation="parity",
            profile="balanced",
            size_class="small",
            trial=trial,
            warm=trial > 0,
            duration_ns=1000 + trial * 10,
            input_digest="1" * 64,
            output_digest="2" * 64,
            correctness_passed=True,
        )
        for trial in range(6)
    ]
    return summarize_trials(rows)


def generate(output_dir: Path) -> None:
    data = fixture()
    defs = definitions(data)
    definition_rows = _definition_rows(defs)
    events = decode_payment_events(data["events"])
    expected_features = project_feature_rows(
        definition_rows,
        data["events"],
        CUSTOMERS,
        "stage5-candidate",
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    actual_features = materialize(
        "stage5-candidate",
        defs,
        events,
        CUSTOMERS,
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200020,
    )
    expected_online = project_online_records(
        definition_rows,
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
        actual_features,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    observed = tuple(row.as_dict() for row in plan.records)
    exhaustive = compare_online_records(expected_online, observed)
    strata = {"c-1": "hot", "c-2": "boundary", "c-empty": "sparse"}
    sample = deterministic_stratified_sample(expected_online, strata, seed=73, per_stratum=2)
    diagnostic = compare_online_records(expected_online, observed, sample_keys=sample)

    bad = [dict(row) for row in observed]
    bad[0] = dict(bad[0]) | {"value": {"integer": 999, "kind": "integer"}}
    semantic_failure = compare_online_records(expected_online, bad)
    stale_failure = compare_online_records(
        expected_online,
        observed,
        request_time=200031,
        maximum_freshness_age=10,
    )

    with tempfile.TemporaryDirectory(prefix="featureforge-stage5-proof-") as temp:
        store = LocalOnlineStore(Path(temp) / "online.sqlite")
        receipt = store.materialize(plan)
        evidence_digests = (exhaustive["report_digest"], receipt["receipt_digest"])
        rejected = evaluate_activation(
            replace(
                _gate_inputs(
                    plan.generation_id,
                    "activate-rejected",
                    receipt["receipt_digest"],
                    expected_generation=None,
                    expected_version=0,
                    evidence_digests=evidence_digests,
                ),
                parity_passed=False,
            )
        )
        store.record_activation_decision(rejected.as_dict())
        pointer_after_rejection = store.pointer(FEATURE_SET)
        eligible = evaluate_activation(
            _gate_inputs(
                plan.generation_id,
                "activate-stage5",
                receipt["receipt_digest"],
                expected_generation=None,
                expected_version=0,
                evidence_digests=evidence_digests,
            )
        )
        activation = activate_eligible(
            store, eligible, actor="stage5-proof", reason="all-gates-passed"
        )
        replay = activate_eligible(
            store, eligible, actor="stage5-proof", reason="all-gates-passed"
        )
        pointer_after_activation = store.pointer(FEATURE_SET)

        later_plan = build_materialization_plan(
            FEATURE_SET,
            "materialize-later",
            defs,
            materialize(
                "stage5-later",
                defs,
                events,
                CUSTOMERS,
                event_cutoff=data["event_cutoff"],
                knowledge_cutoff=200020,
            ),
            source_digest=SOURCE_DIGEST,
            materialized_at=200031,
        )
        later_receipt = store.materialize(later_plan)
        stale_decision = evaluate_activation(
            _gate_inputs(
                later_plan.generation_id,
                "activate-stale",
                later_receipt["receipt_digest"],
                expected_generation=None,
                expected_version=0,
                evidence_digests=(later_receipt["receipt_digest"],),
            )
        )
        stale_cas_rejected = False
        try:
            activate_eligible(
                store, stale_decision, actor="stage5-proof", reason="stale-must-fail"
            )
        except ActivationConflict:
            stale_cas_rejected = True
        pointer_after_stale = store.pointer(FEATURE_SET)
        persisted = store.activation_decision(eligible.operation_id)
        store.close()

    telemetry = TelemetryEvent(
        event="candidate.activation.committed",
        occurred_at=200040,
        labels={"phase": "activation", "status": "committed"},
        trace={
            "definition_set_digest": plan.definition_set_digest,
            "generation_id": plan.generation_id,
            "operation_id": eligible.operation_id,
            "source_frontier_digest": SOURCE_DIGEST,
        },
        measurements={"record_count": plan.expected_count},
    )
    validate_telemetry(telemetry)
    lags = calculate_lags(
        event_time=200000,
        knowledge_time=200020,
        source_frontier_available_at=200021,
        build_started_monotonic=200022,
        build_completed_monotonic=200025,
        materialization_started_monotonic=200026,
        materialization_completed_monotonic=200030,
        candidate_validated_at=200031,
        activated_at=200040,
        request_time=200041,
        record_frontier=200020,
        expires_at=286400,
    )

    platform: dict[str, Any] = {
        "activation": {
            "decision": eligible.as_dict(),
            "receipt": activation,
            "replay_equal": replay == activation,
            "persisted_equal": persisted == eligible.as_dict(),
            "pointer": list(pointer_after_activation),
        },
        "benchmark_contract_proof": _benchmark_shape(),
        "checks": {
            "exhaustive_parity_passed": exhaustive["passed"],
            "independent_features_equal": sorted(
                expected_features,
                key=lambda row: (row["customer_id"], row["feature_name"]),
            )
            == sorted(
                (row.as_dict() for row in actual_features),
                key=lambda row: (row["customer_id"], row["feature_name"]),
            ),
            "policy_bound_to_receipt": activation.get("policy_decision_digest")
            == eligible.decision_digest,
            "replay_idempotent": replay == activation,
            "stratified_diagnostic_passed": diagnostic["passed"],
            "telemetry_valid": True,
        },
        "contract": "featureforge-stage5-platform-proof-v1",
        "diagnostic_parity": diagnostic,
        "exhaustive_parity": exhaustive,
        "lags": lags.as_dict(),
        "project": PROJECT,
        "sample_keys": [list(key) for key in sample],
        "stage": "part2-stage3-global-stage5",
        "telemetry": telemetry.as_dict(),
    }
    platform["proof_digest"] = digest(platform)

    gate_failures: dict[str, Any] = {}
    for field in (
        "source_complete",
        "manifests_valid",
        "offline_valid",
        "online_reconciled",
        "parity_passed",
        "freshness_passed",
        "candidate_consistent",
        "unique_candidate",
        "pointer_current",
    ):
        decision = evaluate_activation(
            replace(
                _gate_inputs(
                    plan.generation_id,
                    f"reject-{field}",
                    receipt["receipt_digest"],
                    expected_generation=None,
                    expected_version=0,
                    evidence_digests=evidence_digests,
                ),
                **{field: False},
            )
        )
        gate_failures[field] = list(decision.reason_codes)
    recovery: dict[str, Any] = {
        "checks": {
            "all_nine_gates_fail_closed": len(gate_failures) == 9
            and all(len(value) == 1 for value in gate_failures.values()),
            "rejected_pointer_preserved": pointer_after_rejection == (None, 0),
            "semantic_fault_detected": "SEMANTIC_MISMATCH"
            in semantic_failure["reasons"],
            "stale_records_detected": "INELIGIBLE_RECORD" in stale_failure["reasons"],
            "stale_publication_rejected": stale_cas_rejected,
            "successful_repair_activated": pointer_after_activation
            == (plan.generation_id, 1),
            "stale_failure_preserved_winner": pointer_after_stale
            == pointer_after_activation,
        },
        "contract": "featureforge-stage5-recovery-proof-v1",
        "gate_failures": gate_failures,
        "project": PROJECT,
        "semantic_failure": semantic_failure,
        "stage": "part2-stage3-global-stage5",
        "stale_failure": stale_failure,
    }
    recovery["proof_digest"] = digest(recovery)
    _write(output_dir / "platform-proof.json", platform)
    _write(output_dir / "failure-recovery-proof.json", recovery)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    generate(args.output_dir)
    print(f"wrote deterministic Stage 5 proofs to {args.output_dir}")


if __name__ == "__main__":
    main()
