#!/usr/bin/env python3
"""Generate deterministic Stage 4 online behavior and recovery evidence."""

from __future__ import annotations

import argparse
import tempfile
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

import boto3
import botocore
import jmespath
import s3transfer

from featureforge.canonical import canonical_json, digest
from featureforge.computation import materialize
from featureforge.model import FeatureDefinition
from featureforge.online import (
    AcknowledgementLost,
    ActivationConflict,
    CandidateValidationError,
    LocalOnlineStore,
    MaterializationInterrupted,
    ReaderToken,
    RetryPolicy,
    activation_transaction_request,
    build_materialization_plan,
    capacity_report,
    classify_dynamodb_error,
    put_candidate_validation_request,
    put_record_request,
    query_generation_request,
    validate_dynamodb_request,
)
from featureforge.provenance import decode_payment_events
from featureforge.spark_runtime import generate_workload
from tests.stage2_support import definitions, fixture
from tests.stage4_oracle import oracle_online_records

PROJECT = "featureforge-ml-feature-platform"
TABLE = "featureforge-online"
FEATURE_SET = "payment-risk-v1"
SOURCE_DIGEST = "a" * 64


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def _definition_rows(rows: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in rows
    ]


def _plan(
    generation_id: str,
    operation_id: str,
    knowledge_cutoff: int,
) -> Any:
    data = fixture()
    defs = definitions(data)
    values = materialize(
        generation_id,
        defs,
        decode_payment_events(data["events"]),
        ("c-1", "c-2", "c-empty"),
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=knowledge_cutoff,
    )
    return build_materialization_plan(
        FEATURE_SET,
        operation_id,
        defs,
        values,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )


def _request_proof(plan: Any, receipt: dict[str, Any]) -> dict[str, Any]:
    requests = {
        "candidate": put_candidate_validation_request(TABLE, plan, receipt),
        "initial_activation": activation_transaction_request(
            TABLE,
            feature_set=FEATURE_SET,
            generation_id=plan.generation_id,
            validation_receipt_digest=receipt["receipt_digest"],
            expected_generation=None,
            expected_version=0,
            operation_id=f"activate-{plan.generation_id}",
        ),
        "page": query_generation_request(
            TABLE,
            ReaderToken(FEATURE_SET, plan.generation_id, 1),
            "c-1",
            page_size=2,
        ),
        "record": put_record_request(TABLE, plan.records[0]),
    }
    operations = {
        "candidate": "PutItem",
        "initial_activation": "TransactWriteItems",
        "page": "Query",
        "record": "PutItem",
    }
    for name, request in requests.items():
        validate_dynamodb_request(operations[name], request)
    return {
        "all_service_model_valid": True,
        "request_digests": {name: digest(request) for name, request in sorted(requests.items())},
        "transaction_action_count": len(requests["initial_activation"]["TransactItems"]),
    }


def _capacity_profiles(defs: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for profile, customer_count, event_count in (
        ("balanced", 12, 48),
        ("skewed", 24, 120),
        ("high-cardinality", 40, 160),
    ):
        workload = generate_workload(
            profile, seed=73, customers=customer_count, events=event_count
        )
        values = materialize(
            f"online-{profile}",
            defs,
            workload.events,
            workload.customer_ids,
            event_cutoff=300000,
            knowledge_cutoff=300000,
        )
        plan = build_materialization_plan(
            FEATURE_SET,
            f"materialize-{profile}",
            defs,
            values,
            source_digest=workload.workload_digest,
            materialized_at=300001,
        )
        report = capacity_report(plan)
        result.append(
            {
                "customer_count": customer_count,
                "event_count": event_count,
                "profile": profile,
                "reconciliation_record_count": plan.expected_count,
                "report": report,
                "retry_amplification_at_four_attempts": report["put_request_count"] * 4,
                "seed": 73,
                "workload_digest": workload.workload_digest,
            }
        )
    return result


def generate_behavior(output: Path) -> None:
    data = fixture()
    defs = definitions(data)
    plan = _plan("generation-online", "materialize-online", 200005)
    expected = oracle_online_records(
        _definition_rows(defs),
        [value.as_dict() for value in materialize(
            plan.generation_id,
            defs,
            decode_payment_events(data["events"]),
            ("c-1", "c-2", "c-empty"),
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=200005,
        )],
        feature_set=FEATURE_SET,
        operation_id=plan.operation_id,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    if tuple(record.as_dict() for record in plan.records) != expected:
        raise RuntimeError("online record envelope diverges from the independent oracle")

    with tempfile.TemporaryDirectory(prefix="featureforge-stage4-behavior-") as temp:
        store = LocalOnlineStore(Path(temp) / "online.sqlite")
        receipt = store.materialize(plan)
        candidate_isolated = store.pointer(FEATURE_SET) == (None, 0)
        pages = [
            len(store.candidate_page(plan.generation_id, page_size=4, after=None)[0])
        ]
        cursor: tuple[str, str] | None = None
        pages = []
        while True:
            page, cursor = store.candidate_page(
                plan.generation_id, page_size=4, after=cursor
            )
            pages.append(len(page))
            if cursor is None:
                break
        activation = store.activate(
            plan.generation_id,
            validation_receipt_digest=receipt["receipt_digest"],
            expected_generation=None,
            expected_version=0,
            operation_id="activate-online",
        )
        token = store.pin_reader(FEATURE_SET)
        history_count = len(store.history(FEATURE_SET))
        record = next(row for row in plan.records if row.value["kind"] != "null")
        outcomes = {
            "expired": store.read(
                token,
                record.customer_id,
                record.feature_name,
                definition_digest=record.definition_digest,
                request_time=record.expires_at,
            ).status,
            "found": store.read(
                token,
                record.customer_id,
                record.feature_name,
                definition_digest=record.definition_digest,
                request_time=record.expires_at - 1,
            ).status,
            "stale": store.read(
                token,
                record.customer_id,
                record.feature_name,
                definition_digest=record.definition_digest,
                request_time=record.knowledge_cutoff + 11,
                maximum_freshness_age=10,
            ).status,
        }
        request_proof = _request_proof(plan, receipt)
        store.close()

    profiles = _capacity_profiles(defs)
    proof: dict[str, Any] = {
        "activation": {
            "history_count": history_count,
            "pointer_version": activation["pointer_version"],
            "receipt_digest": activation["receipt_digest"],
        },
        "capacity_profiles": profiles,
        "checks": {
            "candidate_isolated_before_activation": candidate_isolated,
            "exact_reconciliation": receipt["actual_digest"] == plan.records_digest,
            "independent_oracle_equal": True,
            "item_limit_respected": all(
                profile["report"]["maximum_item_headroom_bytes"] > 0
                for profile in profiles
            ),
            "pinned_generation": token.generation_id == plan.generation_id,
            "request_shapes_valid": request_proof["all_service_model_valid"],
            "typed_serving": outcomes
            == {"expired": "EXPIRED", "found": "FOUND", "stale": "STALE"},
        },
        "contract": "featureforge-stage4-online-proof-v1",
        "oracle_records_digest": digest(expected),
        "pagination_page_counts": pages,
        "plan": plan.as_dict(),
        "project": PROJECT,
        "request_model": request_proof,
        "runtime": {
            "boto3": boto3.__version__,
            "botocore": botocore.__version__,
            "jmespath": jmespath.__version__,
            "python_requirement": ">=3.11",
            "s3transfer": s3transfer.__version__,
            "storage_proof": "file-backed-sqlite",
        },
        "serving_outcomes": outcomes,
        "stage": "part2-stage2-global-stage4",
        "validation_receipt": receipt,
    }
    proof["proof_digest"] = digest(proof)
    _write(output, proof)


def _race(database: Path, plans: list[Any], receipts: list[dict[str, Any]]) -> list[str]:
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    outcomes: list[str] = []

    def publish(index: int) -> None:
        store = LocalOnlineStore(database)
        barrier.wait()
        try:
            store.activate(
                plans[index].generation_id,
                validation_receipt_digest=receipts[index]["receipt_digest"],
                expected_generation="generation-base",
                expected_version=1,
                operation_id=f"activate-race-{index}",
            )
        except ActivationConflict:
            outcome = "semantic-conflict"
        else:
            outcome = "committed"
        finally:
            store.close()
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=publish, args=(index,)) for index in (0, 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return sorted(outcomes)


def generate_recovery(output: Path) -> None:
    plan = _plan("generation-recovered", "materialize-recovered", 200005)
    with tempfile.TemporaryDirectory(prefix="featureforge-stage4-recovery-") as temp:
        root = Path(temp)
        direct = LocalOnlineStore(root / "direct.sqlite")
        direct_receipt = direct.materialize(plan)
        direct.close()

        recovered = LocalOnlineStore(root / "recovered.sqlite")
        try:
            recovered.materialize(plan, interrupt_after=4)
        except MaterializationInterrupted:
            interruption_isolated = recovered.candidate_status(plan.generation_id) == "WRITING"
        else:
            interruption_isolated = False
        policy = RetryPolicy(4, 100, 800, 73)
        delays = policy.delays()
        recovered.record_retry_attempt(
            plan.operation_id,
            attempt=1,
            outcome="RETRYABLE",
            confirmed_count=4,
            next_retry_at=200030 + delays[0],
            terminal_reason="partial-throttle",
        )
        first_page, cursor = recovered.candidate_page(
            plan.generation_id, page_size=3
        )
        recovered.close()
        reopened = LocalOnlineStore(root / "recovered.sqlite")
        second_page, _ = reopened.candidate_page(
            plan.generation_id, page_size=3, after=cursor
        )
        recovered_receipt = reopened.materialize(plan)
        reopened.record_retry_attempt(
            plan.operation_id,
            attempt=2,
            outcome="CONFIRMED",
            confirmed_count=plan.expected_count,
            next_retry_at=None,
            terminal_reason=None,
        )
        duplicate_receipt = reopened.materialize(plan)
        retry_ledger = reopened.retry_attempts(plan.operation_id)
        reopened.close()

        lost = LocalOnlineStore(root / "lost-ack.sqlite")
        base = _plan("generation-base", "materialize-base", 180000)
        base_receipt = lost.materialize(base)
        lost.activate(
            base.generation_id,
            validation_receipt_digest=base_receipt["receipt_digest"],
            expected_generation=None,
            expected_version=0,
            operation_id="activate-base",
        )
        target = _plan("generation-target", "materialize-target", 200005)
        target_receipt = lost.materialize(target)
        try:
            lost.activate(
                target.generation_id,
                validation_receipt_digest=target_receipt["receipt_digest"],
                expected_generation=base.generation_id,
                expected_version=1,
                operation_id="activate-target",
                lose_acknowledgement=True,
            )
        except AcknowledgementLost:
            acknowledgement_lost = True
        else:
            acknowledgement_lost = False
        lost.close()
        replayed = LocalOnlineStore(root / "lost-ack.sqlite")
        activation_replay = replayed.activate(
            target.generation_id,
            validation_receipt_digest=target_receipt["receipt_digest"],
            expected_generation=base.generation_id,
            expected_version=1,
            operation_id="activate-target",
        )
        history_after_replay = len(replayed.history(FEATURE_SET))
        replayed.close()

        race_store = LocalOnlineStore(root / "race.sqlite")
        race_base = _plan("generation-base", "materialize-race-base", 180000)
        race_base_receipt = race_store.materialize(race_base)
        race_store.activate(
            race_base.generation_id,
            validation_receipt_digest=race_base_receipt["receipt_digest"],
            expected_generation=None,
            expected_version=0,
            operation_id="activate-race-base",
        )
        race_plans = [
            _plan("generation-race-a", "materialize-race-a", 200005),
            _plan("generation-race-b", "materialize-race-b", 200020),
        ]
        race_receipts = [race_store.materialize(item) for item in race_plans]
        race_store.close()
        race_outcomes = _race(root / "race.sqlite", race_plans, race_receipts)
        race_verified = LocalOnlineStore(root / "race.sqlite")
        race_pointer_version = race_verified.pointer(FEATURE_SET)[1]
        race_history_count = len(race_verified.history(FEATURE_SET))
        race_verified.close()

        corrupt = LocalOnlineStore(root / "corrupt.sqlite")
        corrupt.begin_materialization(plan)
        for record in plan.records:
            corrupt.write_record(plan, record)
        corrupt.connection.execute(
            """UPDATE records SET record_json='{}'
               WHERE generation_id=? AND customer_id=? AND feature_name=?""",
            (
                plan.records[0].generation_id,
                plan.records[0].customer_id,
                plan.records[0].feature_name,
            ),
        )
        try:
            corrupt.reconcile(plan)
        except CandidateValidationError:
            corruption_rejected = True
        else:
            corruption_rejected = False
        corrupt.close()

    recovery: dict[str, Any] = {
        "activation_lost_ack": {
            "acknowledgement_lost": acknowledgement_lost,
            "history_after_replay": history_after_replay,
            "replayed_receipt_digest": activation_replay["receipt_digest"],
        },
        "checks": {
            "corruption_rejected": corruption_rejected,
            "duplicate_dispatch_idempotent": duplicate_receipt == recovered_receipt,
            "interruption_isolated": interruption_isolated,
            "lost_ack_replayed_once": acknowledgement_lost and history_after_replay == 2,
            "pagination_restart_complete": (
                len(first_page) + len(second_page) == 4
                and not {
                    (row["customer_id"], row["feature_name"]) for row in first_page
                }
                & {
                    (row["customer_id"], row["feature_name"]) for row in second_page
                }
            ),
            "recovered_equals_uninterrupted": (
                recovered_receipt["actual_digest"] == direct_receipt["actual_digest"]
            ),
            "retry_ledger_durable": len(retry_ledger) == 2,
            "two_publishers_one_winner": race_outcomes
            == ["committed", "semantic-conflict"],
        },
        "contract": "featureforge-stage4-recovery-proof-v1",
        "error_classification": {
            "access_denied": classify_dynamodb_error("AccessDeniedException"),
            "conditional": classify_dynamodb_error("ConditionalCheckFailedException"),
            "malformed_unknown": classify_dynamodb_error("MalformedUnprocessedItems"),
            "throttle": classify_dynamodb_error("ThrottlingException"),
        },
        "materialization": {
            "direct_digest": direct_receipt["actual_digest"],
            "recovered_digest": recovered_receipt["actual_digest"],
            "restart_first_page_count": len(first_page),
            "restart_second_page_count": len(second_page),
        },
        "project": PROJECT,
        "race": {
            "history_count": race_history_count,
            "outcomes": race_outcomes,
            "pointer_version": race_pointer_version,
        },
        "retry": {
            "delays_ms": delays,
            "ledger": retry_ledger,
            "maximum_attempts": policy.maximum_attempts,
            "seed": policy.seed,
        },
        "stage": "part2-stage2-global-stage4",
    }
    recovery["proof_digest"] = digest(recovery)
    _write(output, recovery)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    generate_behavior(args.output_dir / "online-proof.json")
    generate_recovery(args.output_dir / "failure-recovery-proof.json")
    print(f"wrote deterministic Stage 4 proofs to {args.output_dir}")


if __name__ == "__main__":
    main()
