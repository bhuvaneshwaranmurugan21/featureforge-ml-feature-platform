from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest

from featureforge.canonical import digest
from featureforge.computation import materialize
from featureforge.model import FeatureDefinition, FeatureValue
from featureforge.online import (
    AcknowledgementLost,
    ActivationConflict,
    CandidateValidationError,
    DynamoDBOnlineBoundary,
    LocalOnlineStore,
    MaterializationInterrupted,
    OnlineConflict,
    OnlineContractError,
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
from tests.stage2_support import definitions, fixture
from tests.stage4_oracle import oracle_online_records

TABLE = "featureforge-online"
FEATURE_SET = "payment-risk-v1"
SOURCE_DIGEST = "a" * 64


def _values(
    generation_id: str, knowledge_cutoff: int
) -> tuple[tuple[FeatureDefinition, ...], tuple[FeatureValue, ...]]:
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
    return defs, values


def _plan(
    generation_id: str,
    operation_id: str,
    knowledge_cutoff: int = 200005,
):
    defs, values = _values(generation_id, knowledge_cutoff)
    return build_materialization_plan(
        FEATURE_SET,
        operation_id,
        defs,
        values,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )


def _definition_rows(defs: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in defs
    ]


def test_records_match_independent_primitive_oracle() -> None:
    defs, values = _values("generation-a", 200005)
    plan = build_materialization_plan(
        FEATURE_SET,
        "materialize-a",
        defs,
        values,
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    expected = oracle_online_records(
        _definition_rows(defs),
        [value.as_dict() for value in values],
        feature_set=FEATURE_SET,
        operation_id="materialize-a",
        source_digest=SOURCE_DIGEST,
        materialized_at=200030,
    )
    assert tuple(record.as_dict() for record in plan.records) == expected


def test_canonical_values_reject_nonfinite_and_mismatched_types() -> None:
    definition = FeatureDefinition(
        "ratio", 1, "float", 60, "failed_payment_ratio", 60, "ratio"
    )
    bad = FeatureValue("c", "ratio", float("nan"), 10, 10, definition.definition_digest, "g")
    with pytest.raises(OnlineContractError, match="nonfinite"):
        build_materialization_plan(
            FEATURE_SET,
            "op",
            (definition,),
            (bad,),
            source_digest=SOURCE_DIGEST,
            materialized_at=10,
        )
    integer = replace(definition, name="count", value_type="integer")
    wrong = FeatureValue(
        "c", "count", 1.5, 10, 10, integer.definition_digest, "g"
    )
    with pytest.raises(OnlineContractError, match="non-integer"):
        build_materialization_plan(
            FEATURE_SET,
            "op",
            (integer,),
            (wrong,),
            source_digest=SOURCE_DIGEST,
            materialized_at=10,
        )


def test_duplicate_entity_feature_and_mixed_generation_fail_closed() -> None:
    defs, values = _values("generation-a", 200005)
    with pytest.raises(OnlineContractError, match="duplicate"):
        build_materialization_plan(
            FEATURE_SET,
            "op",
            defs,
            (*values, values[0]),
            source_digest=SOURCE_DIGEST,
            materialized_at=200030,
        )

    with pytest.raises(OnlineContractError, match="non-empty"):
        build_materialization_plan(
            FEATURE_SET,
            "op",
            defs,
            (),
            source_digest=SOURCE_DIGEST,
            materialized_at=200030,
        )
    mixed = (*values[:-1], replace(values[-1], generation_id="generation-b"))
    with pytest.raises(OnlineContractError, match="exactly one generation"):
        build_materialization_plan(
            FEATURE_SET,
            "op",
            defs,
            mixed,
            source_digest=SOURCE_DIGEST,
            materialized_at=200030,
        )


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def put_item(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("PutItem", request))
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def query(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("Query", request))
        return {"Items": []}

    def transact_write_items(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("TransactWriteItems", request))
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


def test_dynamodb_requests_validate_against_pinned_service_model() -> None:
    plan = _plan("generation-a", "materialize-a")
    record = plan.records[0]
    put = put_record_request(TABLE, record)
    query = query_generation_request(
        TABLE, ReaderToken(FEATURE_SET, "generation-a", 1), "c-1"
    )
    activation = activation_transaction_request(
        TABLE,
        feature_set=FEATURE_SET,
        generation_id="generation-a",
        validation_receipt_digest="b" * 64,
        expected_generation=None,
        expected_version=0,
        operation_id="activate-a",
    )
    validate_dynamodb_request("PutItem", put)
    validate_dynamodb_request("Query", query)
    validate_dynamodb_request("TransactWriteItems", activation)
    assert "attribute_not_exists" in put["ConditionExpression"]
    assert len(activation["TransactItems"]) == 3
    assert activation["TransactItems"][1]["Update"]["ConditionExpression"].startswith(
        "attribute_not_exists"
    )


def test_candidate_control_and_paginated_query_requests_validate() -> None:
    plan = _plan("generation-a", "materialize-a")
    store = LocalOnlineStore(":memory:")
    receipt = store.materialize(plan)
    control = put_candidate_validation_request(TABLE, plan, receipt)
    page_key = {"PK": {"S": "entity"}, "SK": {"S": "feature"}}
    query = query_generation_request(
        TABLE,
        ReaderToken(FEATURE_SET, "generation-a", 1),
        "c-1",
        exclusive_start_key=page_key,
        page_size=2,
    )
    validate_dynamodb_request("PutItem", control)
    validate_dynamodb_request("Query", query)
    assert control["Item"]["state"] == {"S": "VALIDATED"}
    assert query["ExclusiveStartKey"] == page_key
    assert query["Limit"] == 2


def test_production_boundary_dispatches_only_validated_shapes() -> None:
    plan = _plan("generation-a", "materialize-a")
    client = RecordingClient()
    boundary = DynamoDBOnlineBoundary(TABLE, client)
    boundary.put_record(plan.records[0])
    local = LocalOnlineStore(":memory:")
    receipt = local.materialize(plan)
    boundary.put_candidate_validation(plan, receipt)
    boundary.query_generation(
        ReaderToken(FEATURE_SET, "generation-a", 1), "c-1"
    )
    boundary.activate(
        feature_set=FEATURE_SET,
        generation_id="generation-a",
        validation_receipt_digest="b" * 64,
        expected_generation=None,
        expected_version=0,
        operation_id="activate-a",
    )
    assert [name for name, _ in client.calls] == [
        "PutItem",
        "PutItem",
        "Query",
        "TransactWriteItems",
    ]


def test_materialization_is_exact_isolated_and_idempotent(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    receipt = store.materialize(plan)
    assert receipt["actual_count"] == 15
    assert receipt["actual_digest"] == plan.records_digest
    assert store.candidate_status("generation-a") == "VALIDATED"
    assert store.pointer(FEATURE_SET) == (None, 0)
    assert store.materialize(plan) == receipt
    assert store.history(FEATURE_SET) == ()


def test_candidate_pagination_is_complete_stable_and_restartable(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    store.materialize(plan)
    all_rows = store.candidate_records("generation-a", page_size=4)
    first, cursor = store.candidate_page("generation-a", page_size=4)
    assert cursor is not None
    store.close()
    reopened = LocalOnlineStore(tmp_path / "online.sqlite")
    second, _ = reopened.candidate_page(
        "generation-a", page_size=4, after=cursor
    )
    assert len(all_rows) == 15
    assert tuple(all_rows[:4]) == first
    assert tuple(all_rows[4:8]) == second
    assert len({(row["customer_id"], row["feature_name"]) for row in all_rows}) == 15


def test_operation_and_record_conflicts_fail_closed(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    store.begin_materialization(plan)
    store.write_record(plan, plan.records[0])
    changed = replace(plan.records[0], record_digest="0" * 64)
    with pytest.raises(OnlineContractError, match="intact"):
        store.write_record(plan, changed)
    conflicting = _plan("generation-a", "another-operation", 200020)
    with pytest.raises(OnlineConflict):
        store.begin_materialization(conflicting)


def test_interruption_restart_and_partial_activation_rejection(tmp_path: Path) -> None:
    database = tmp_path / "online.sqlite"
    plan = _plan("generation-a", "materialize-a")
    first = LocalOnlineStore(database)
    with pytest.raises(MaterializationInterrupted):
        first.materialize(plan, interrupt_after=4)
    assert first.candidate_status("generation-a") == "WRITING"
    first.close()
    reopened = LocalOnlineStore(database)
    with pytest.raises(ActivationConflict):
        reopened.activate(
            "generation-a",
            validation_receipt_digest="0" * 64,
            expected_generation=None,
            expected_version=0,
            operation_id="premature",
        )
    receipt = reopened.materialize(plan)
    assert receipt["actual_digest"] == plan.records_digest
    assert reopened.candidate_status("generation-a") == "VALIDATED"


def test_partial_and_extra_candidates_fail_exact_reconciliation(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    store.begin_materialization(plan)
    for record in plan.records[:-1]:
        store.write_record(plan, record)
    with pytest.raises(CandidateValidationError, match="incomplete"):
        store.reconcile(plan)
    store.write_record(plan, plan.records[-1])
    store.connection.execute("PRAGMA foreign_keys = OFF")
    extra = plan.records[0]
    store.connection.execute(
        """INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "generation-a",
            "unexpected",
            "unexpected",
            extra.definition_digest,
            json.dumps(extra.as_dict(), sort_keys=True, separators=(",", ":")),
            extra.record_digest,
            extra.expires_at,
        ),
    )
    with pytest.raises(CandidateValidationError, match="incomplete"):
        store.reconcile(plan)


def test_tamper_before_validation_and_after_validation_is_detected(tmp_path: Path) -> None:
    database = tmp_path / "online.sqlite"
    store = LocalOnlineStore(database)
    plan = _plan("generation-a", "materialize-a")
    store.begin_materialization(plan)
    for record in plan.records:
        store.write_record(plan, record)
    row = store.connection.execute(
        "SELECT record_json FROM records WHERE generation_id=? LIMIT 1",
        ("generation-a",),
    ).fetchone()
    body = json.loads(row["record_json"])
    body["materialized_at"] += 1
    store.connection.execute(
        """UPDATE records SET record_json=?
           WHERE generation_id=? AND customer_id=? AND feature_name=?""",
        (
            json.dumps(body, sort_keys=True, separators=(",", ":")),
            "generation-a",
            body["customer_id"],
            body["feature_name"],
        ),
    )
    with pytest.raises(CandidateValidationError, match="corrupt"):
        store.reconcile(plan)
    store.close()

    clean = LocalOnlineStore(tmp_path / "validated.sqlite")
    receipt = clean.materialize(plan)
    row = clean.connection.execute(
        "SELECT record_json FROM records WHERE generation_id=? LIMIT 1",
        ("generation-a",),
    ).fetchone()
    body = json.loads(row["record_json"])
    body["materialized_at"] += 1
    clean.connection.execute(
        """UPDATE records SET record_json=?
           WHERE generation_id=? AND customer_id=? AND feature_name=?""",
        (
            json.dumps(body, sort_keys=True, separators=(",", ":")),
            "generation-a",
            body["customer_id"],
            body["feature_name"],
        ),
    )
    with pytest.raises(ActivationConflict, match="changed"):
        clean.activate(
            "generation-a",
            validation_receipt_digest=receipt["receipt_digest"],
            expected_generation=None,
            expected_version=0,
            operation_id="activate-a",
        )


def _activate_base(store: LocalOnlineStore) -> tuple[Any, dict[str, Any]]:
    plan = _plan("generation-base", "materialize-base", 180000)
    receipt = store.materialize(plan)
    activation = store.activate(
        "generation-base",
        validation_receipt_digest=receipt["receipt_digest"],
        expected_generation=None,
        expected_version=0,
        operation_id="activate-base",
    )
    return plan, activation


def test_guarded_activation_lost_ack_and_pinned_reader(tmp_path: Path) -> None:
    database = tmp_path / "online.sqlite"
    store = LocalOnlineStore(database)
    base, _ = _activate_base(store)
    old_token = store.pin_reader(FEATURE_SET)
    next_plan = _plan("generation-next", "materialize-next", 200005)
    receipt = store.materialize(next_plan)
    with pytest.raises(AcknowledgementLost):
        store.activate(
            "generation-next",
            validation_receipt_digest=receipt["receipt_digest"],
            expected_generation="generation-base",
            expected_version=1,
            operation_id="activate-next",
            lose_acknowledgement=True,
        )
    store.close()
    reopened = LocalOnlineStore(database)
    replay = reopened.activate(
        "generation-next",
        validation_receipt_digest=receipt["receipt_digest"],
        expected_generation="generation-base",
        expected_version=1,
        operation_id="activate-next",
    )
    assert replay["pointer_version"] == 2
    assert replay["actor"] == "stage4-local-proof"
    assert replay["reason"] == "validated-candidate-promotion"
    new_token = reopened.pin_reader(FEATURE_SET)
    target = next(record for record in base.records if record.customer_id == "c-1")
    old_result = reopened.read(
        old_token,
        target.customer_id,
        target.feature_name,
        definition_digest=target.definition_digest,
        request_time=target.expires_at - 1,
    )
    new_record = next(
        record
        for record in next_plan.records
        if record.customer_id == target.customer_id
        and record.feature_name == target.feature_name
    )
    new_result = reopened.read(
        new_token,
        new_record.customer_id,
        new_record.feature_name,
        definition_digest=new_record.definition_digest,
        request_time=new_record.expires_at - 1,
    )
    assert old_result.generation_id == "generation-base"
    assert new_result.generation_id == "generation-next"
    assert old_token.generation_id != new_token.generation_id
    assert len(reopened.history(FEATURE_SET)) == 2
    assert reopened.history(FEATURE_SET)[-1]["actor"] == "stage4-local-proof"


def test_reconciliation_cannot_demote_an_active_candidate(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    receipt = store.materialize(plan)
    store.activate(
        plan.generation_id,
        validation_receipt_digest=receipt["receipt_digest"],
        expected_generation=None,
        expected_version=0,
        operation_id="activate-a",
    )
    assert store.reconcile(plan) == receipt
    assert store.candidate_status(plan.generation_id) == "ACTIVE"


def test_two_publishers_have_one_semantic_cas_winner(tmp_path: Path) -> None:
    database = tmp_path / "online.sqlite"
    setup = LocalOnlineStore(database)
    _activate_base(setup)
    plans = [
        _plan("generation-a", "materialize-a", 200005),
        _plan("generation-b", "materialize-b", 200020),
    ]
    receipts = [setup.materialize(plan) for plan in plans]
    setup.close()
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    lock = threading.Lock()

    def publish(index: int) -> None:
        contender = LocalOnlineStore(database)
        barrier.wait()
        try:
            contender.activate(
                plans[index].generation_id,
                validation_receipt_digest=receipts[index]["receipt_digest"],
                expected_generation="generation-base",
                expected_version=1,
                operation_id=f"activate-{index}",
            )
        except ActivationConflict:
            outcome = "semantic-conflict"
        else:
            outcome = "committed"
        finally:
            contender.close()
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=publish, args=(index,)) for index in (0, 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["committed", "semantic-conflict"]
    verified = LocalOnlineStore(database)
    assert verified.pointer(FEATURE_SET)[1] == 2
    assert len(verified.history(FEATURE_SET)) == 2


def test_typed_serving_outcomes_and_exact_ttl_boundary(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan, _ = _activate_base(store)
    token = store.pin_reader(FEATURE_SET)
    record = next(
        item
        for item in plan.records
        if item.customer_id == "c-1" and item.value["kind"] != "null"
    )
    before = store.read(
        token,
        record.customer_id,
        record.feature_name,
        definition_digest=record.definition_digest,
        request_time=record.expires_at - 1,
    )
    assert before.status == "FOUND"
    assert (
        store.read(
            token,
            record.customer_id,
            record.feature_name,
            definition_digest=record.definition_digest,
            request_time=record.expires_at,
        ).status
        == "EXPIRED"
    )
    assert (
        store.read(
            token,
            record.customer_id,
            "missing-feature",
            definition_digest=record.definition_digest,
            request_time=record.expires_at - 1,
        ).status
        == "ABSENT"
    )
    assert (
        store.read(
            token,
            record.customer_id,
            record.feature_name,
            definition_digest="0" * 64,
            request_time=record.expires_at - 1,
        ).status
        == "DEFINITION_MISMATCH"
    )
    assert (
        store.read(
            token,
            record.customer_id,
            record.feature_name,
            definition_digest=record.definition_digest,
            request_time=record.knowledge_cutoff + 11,
            maximum_freshness_age=10,
        ).status
        == "STALE"
    )
    assert (
        store.read(
            token,
            record.customer_id,
            record.feature_name,
            definition_digest=record.definition_digest,
            request_time=record.knowledge_cutoff + 10,
            maximum_freshness_age=10,
        ).status
        == "FOUND"
    )
    unavailable = ReaderToken(FEATURE_SET, "does-not-exist", 99)
    assert (
        store.read(
            unavailable,
            record.customer_id,
            record.feature_name,
            definition_digest=record.definition_digest,
            request_time=record.expires_at - 1,
        ).status
        == "GENERATION_UNAVAILABLE"
    )


def test_corrupt_and_storage_failure_are_not_absence(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan, _ = _activate_base(store)
    token = store.pin_reader(FEATURE_SET)
    record = plan.records[0]
    store.connection.execute(
        """UPDATE records SET record_json='{}'
           WHERE generation_id=? AND customer_id=? AND feature_name=?""",
        (record.generation_id, record.customer_id, record.feature_name),
    )
    result = store.read(
        token,
        record.customer_id,
        record.feature_name,
        definition_digest=record.definition_digest,
        request_time=record.expires_at - 1,
    )
    assert result.status == "CORRUPT_RECORD"
    valid_body = plan.records[1].as_dict()
    valid_body["value"] = {"kind": "unsupported"}
    body_without_digest = dict(valid_body)
    body_without_digest.pop("record_digest")
    new_digest = digest(body_without_digest)
    valid_body["record_digest"] = new_digest
    store.connection.execute(
        """UPDATE records SET record_json=?, record_digest=?
           WHERE generation_id=? AND customer_id=? AND feature_name=?""",
        (
            json.dumps(valid_body, sort_keys=True, separators=(",", ":")),
            new_digest,
            plan.records[1].generation_id,
            plan.records[1].customer_id,
            plan.records[1].feature_name,
        ),
    )
    assert (
        store.read(
            token,
            plan.records[1].customer_id,
            plan.records[1].feature_name,
            definition_digest=plan.records[1].definition_digest,
            request_time=plan.records[1].expires_at - 1,
        ).status
        == "CORRUPT_RECORD"
    )
    store.close()
    assert (
        store.read(
            token,
            record.customer_id,
            record.feature_name,
            definition_digest=record.definition_digest,
            request_time=record.expires_at - 1,
        ).status
        == "STORAGE_FAILURE"
    )


def test_retry_policy_and_capacity_report_are_bounded_and_deterministic() -> None:
    policy = RetryPolicy(5, 100, 1000, 73)
    assert policy.delays() == RetryPolicy(5, 100, 1000, 73).delays()
    assert len(policy.delays()) == 4
    assert all(100 <= value <= 1000 for value in policy.delays())
    report = capacity_report(_plan("generation-a", "materialize-a"))
    assert report["item_count"] == 15
    assert report["maximum_item_bytes"] < report["service_item_limit_bytes"]
    assert report["maximum_records_per_partition"] == 5


def test_retry_ledger_and_error_classification_are_fail_closed(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    plan = _plan("generation-a", "materialize-a")
    store.begin_materialization(plan)
    assert (
        store.record_retry_attempt(
            plan.operation_id,
            attempt=1,
            outcome="RETRYABLE",
            confirmed_count=4,
            next_retry_at=200123,
            terminal_reason="throttled-subset",
        )
        == "recorded"
    )
    assert (
        store.record_retry_attempt(
            plan.operation_id,
            attempt=1,
            outcome="RETRYABLE",
            confirmed_count=4,
            next_retry_at=200123,
            terminal_reason="throttled-subset",
        )
        == "replayed"
    )
    assert len(store.retry_attempts(plan.operation_id)) == 1
    with pytest.raises(OnlineConflict):
        store.record_retry_attempt(
            plan.operation_id,
            attempt=1,
            outcome="RETRYABLE",
            confirmed_count=3,
            next_retry_at=200123,
            terminal_reason="conflicting-replay",
        )
    assert classify_dynamodb_error("ThrottlingException") == "RETRYABLE"
    assert classify_dynamodb_error("ConditionalCheckFailedException") == "CONFLICT"
    assert classify_dynamodb_error("AccessDeniedException") == "TERMINAL"
    assert classify_dynamodb_error("NovelServiceError") == "UNKNOWN"


def test_contract_files_have_stable_stage4_identities() -> None:
    root = Path(__file__).resolve().parents[1]
    expected = {
        "online-activation-receipt-v1.json": (
            "urn:featureforge:online-activation-receipt:v1"
        ),
        "online-active-pointer-v1.json": "urn:featureforge:online-active-pointer:v1",
        "online-feature-record-v1.json": "urn:featureforge:online-feature-record:v1",
        "online-materialization-plan-v1.json": (
            "urn:featureforge:online-materialization-plan:v1"
        ),
        "online-validation-receipt-v1.json": (
            "urn:featureforge:online-validation-receipt:v1"
        ),
        "online-serving-result-v1.json": "urn:featureforge:online-serving-result:v1",
    }
    for name, identity in expected.items():
        value = json.loads((root / "contracts" / name).read_text(encoding="utf-8"))
        assert value["$id"] == identity


def test_closed_database_raises_sqlite_error_outside_typed_read(tmp_path: Path) -> None:
    store = LocalOnlineStore(tmp_path / "online.sqlite")
    store.close()
    with pytest.raises(sqlite3.ProgrammingError):
        store.pointer(FEATURE_SET)
