"""Boundary-level evidence; never evidence of a live DynamoDB workload."""

from __future__ import annotations

import copy
import hashlib
import io
import threading
from dataclasses import asdict, replace
from typing import Any

import pytest

from featureforge.aws_runtime import DynamoOnlineRuntime, RuntimeConflict, TaskLedger
from featureforge.canonical import canonical_json, digest
from featureforge.control_worker import ControlWorker
from featureforge.expected import project_feature_rows
from featureforge.managed import ManagedRunManifest, S3ObjectAuthority, admit_managed_run
from featureforge.model import FeatureDefinition, FeatureValue, PaymentEvent
from featureforge.online import build_materialization_plan, validate_dynamodb_request
from tests.test_stage6_managed import FakeS3, costs, inventory, lease, manifest


class DatabaseBoundary:
    """Test-only low-level request recorder with durable conditional keys."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.repeat_cursor = False
        self.lose_ack = False
        self.lock = threading.RLock()

    @staticmethod
    def key(table: str, item: dict[str, Any]) -> tuple[str, str, str]:
        return table, item["PK"]["S"], item["SK"]["S"]

    def put_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("PutItem", request)
        self.calls.append(("PutItem", request))
        key = self.key(request["TableName"], request["Item"])
        old = self.items.get(key)
        if old is not None and old != request["Item"]:
            raise RuntimeConflict("conditional immutable conflict")
        self.items[key] = copy.deepcopy(request["Item"])
        return {}

    def scan(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("Scan", request)
        self.calls.append(("Scan", request))
        assert request["ConsistentRead"] is True
        rows = [
            copy.deepcopy(item)
            for (table, _, _), item in self.items.items()
            if table == request["TableName"]
        ]
        if self.repeat_cursor:
            return {"Items": [], "LastEvaluatedKey": {"PK": {"S": "repeat"}}}
        return {"Items": rows}

    def get_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("GetItem", request)
        self.calls.append(("GetItem", request))
        assert request["ConsistentRead"] is True
        item = self.items.get(self.key(request["TableName"], request["Key"]))
        return {} if item is None else {"Item": copy.deepcopy(item)}

    def update_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("UpdateItem", request)
        self.calls.append(("UpdateItem", request))
        with self.lock:
            self._condition(request, self.items)
            key = self.key(request["TableName"], request["Key"])
            self.items[key]["state"] = request["ExpressionAttributeValues"][":sealed"]
            return {"Attributes": copy.deepcopy(self.items[key])}

    def _condition(self, request: dict[str, Any], items: dict[Any, Any]) -> None:
        item = items.get(self.key(request["TableName"], request["Key"]), {})
        values = request["ExpressionAttributeValues"]
        wanted_state = values.get(":state", values.get(":validated"))
        if item.get("state") != wanted_state:
            raise RuntimeConflict("conditional lifecycle state conflict")
        if ":plan" in values and item.get("plan_digest") != values[":plan"]:
            raise RuntimeConflict("conditional plan conflict")
        if (
            ":receipt" in request["ConditionExpression"]
            and item.get("validation_receipt_digest") != values[":receipt"]
        ):
            raise RuntimeConflict("conditional validation conflict")

    def transact_write_items(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("TransactWriteItems", request)
        self.calls.append(("TransactWriteItems", request))
        with self.lock:
            candidate_items = copy.deepcopy(self.items)
            for entry in request["TransactItems"]:
                if "ConditionCheck" in entry:
                    self._condition(entry["ConditionCheck"], candidate_items)
                elif "Put" in entry:
                    put = entry["Put"]
                    key = self.key(put["TableName"], put["Item"])
                    if key in candidate_items and candidate_items[key] != put["Item"]:
                        raise RuntimeConflict("conditional immutable conflict")
                    candidate_items[key] = copy.deepcopy(put["Item"])
                else:
                    update = entry["Update"]
                    key = self.key(update["TableName"], update["Key"])
                    values = update["ExpressionAttributeValues"]
                    if ":generation" in values:
                        if key in candidate_items:
                            raise RuntimeConflict("conditional stale pointer")
                        candidate_items[key] = update["Key"] | {
                            "active_generation": values[":generation"],
                            "pointer_version": values[":new_version"],
                        }
                    else:
                        self._condition(update, candidate_items)
                        candidate_items[key]["state"] = values[":validated"]
                        candidate_items[key]["validation_receipt_digest"] = values[":receipt"]
            self.items = candidate_items
        if self.lose_ack and len(request["TransactItems"]) == 4:
            raise TimeoutError("committed acknowledgement lost")
        return {}


def fixture() -> tuple[ControlWorker, dict[str, Any], DatabaseBoundary]:
    definition = FeatureDefinition(
        "count", 1, "integer", 1000, "transaction_count", 100, "count", 0
    )
    event = PaymentEvent("e1", "c1", 900, 950, 10, "succeeded", 0.1)
    source = {
        "definitions": [asdict(definition)],
        "events": [event.as_dict()],
        "customer_ids": ["c1"],
        "generation_id": "g1",
        "event_cutoff": 1000,
        "knowledge_cutoff": 1000,
    }
    source_bytes = canonical_json(source).encode()
    source_auth = S3ObjectAuthority(
        "source", "source.json", "v1", hashlib.sha256(source_bytes).hexdigest()
    )
    value = replace(manifest(), inputs=(source_auth,), manifest_digest="")
    rows = project_feature_rows(
        [asdict(definition) | {"definition_digest": definition.definition_digest}],
        [event.as_dict()],
        ["c1"],
        "g1",
        event_cutoff=1000,
        knowledge_cutoff=1000,
    )
    row_bytes = canonical_json(rows).encode()
    row_auth = S3ObjectAuthority(
        value.output_bucket,
        value.output_prefix + "rows.json",
        "v1",
        hashlib.sha256(row_bytes).hexdigest(),
    )
    manifest_bytes = canonical_json(value.as_dict()).encode()
    manifest_auth = S3ObjectAuthority(
        "manifest", "manifest.json", "v1", hashlib.sha256(manifest_bytes).hexdigest()
    )
    objects = {
        (item.bucket, item.key, item.version_id): body
        for item, body in (
            (source_auth, source_bytes),
            (row_auth, row_bytes),
            (manifest_auth, manifest_bytes),
        )
    }
    generation_bytes = canonical_json({"generation_id": "g1"}).encode()
    generation_auth = S3ObjectAuthority(
        value.output_bucket,
        value.output_prefix + "manifest.json",
        "v1",
        hashlib.sha256(generation_bytes).hexdigest(),
    )
    output_content = {
        "contract": "stage6-glue-output-authority-v1",
        "generation_id": "g1",
        "input_authority": source_auth.as_dict(),
        "rows_authority": row_auth.as_dict(),
        "manifest_authority": generation_auth.as_dict(),
    }
    output_bytes = canonical_json(
        output_content | {"authority_digest": digest(output_content)}
    ).encode()
    output_auth = S3ObjectAuthority(
        value.output_bucket,
        value.output_prefix + "output-authority.json",
        "v1",
        hashlib.sha256(output_bytes).hexdigest(),
    )
    objects[(generation_auth.bucket, generation_auth.key, generation_auth.version_id)] = (
        generation_bytes
    )
    objects[(output_auth.bucket, output_auth.key, output_auth.version_id)] = output_bytes
    database = DatabaseBoundary()

    class LocalAdmission:
        def verify(self, checked: ManagedRunManifest) -> dict[str, Any]:
            decision = admit_managed_run(
                checked,
                observed_account_fingerprint=checked.account_fingerprint,
                observed_region=checked.region,
                lease=lease(),
                inventory=inventory(),
                available_quotas={"glue-runs": 1},
                required_quotas={"glue-runs": 1},
                cost=costs(),
                observed_at_epoch=1200,
            )
            return asdict(decision) | {
                "admission_digest": decision.admission_digest,
                "run_id": checked.run_id,
            }

        def verify_current(self, checked: ManagedRunManifest, prior: dict[str, Any]) -> None:
            assert self.verify(checked)["admission_digest"] == prior["admission_digest"]

    class VersionedS3(FakeS3):
        def get_object(self, **request: Any) -> dict[str, Any]:
            version = request.get("VersionId", "v1")
            return {
                "VersionId": version,
                "Body": io.BytesIO(self.objects[(request["Bucket"], request["Key"], version)]),
            }

    worker = ControlWorker(
        s3=VersionedS3(objects),
        ledger=TaskLedger(),
        online=DynamoOnlineRuntime(database, "online", "control"),
        admission=LocalAdmission(),
    )
    request = {
        "action": "MATERIALIZE_ONLINE",
        "manifest": value.as_dict(),
        "manifest_authority": manifest_auth.as_dict(),
        "payload": {
            "online_payload_authority": row_auth.as_dict(),
            "feature_set": "risk",
            "materialized_at": 1001,
            "request_time": 1002,
            "maximum_freshness_age": 10,
        },
    }
    worker.dispatch(request | {"action": "VALIDATE", "payload": {"execution_id": "execution-1"}})
    worker.dispatch(
        request
        | {"action": "RECORD_GLUE", "payload": {"glue_job_run_id": "jr-1", "state": "SUCCEEDED"}}
    )
    return worker, request, database


def test_validation_replay_preserves_execution_owner_and_rejects_second_execution() -> None:
    worker, request, database = fixture()
    first = request | {"action": "VALIDATE", "payload": {"execution_id": "execution-1"}}
    receipt = worker.dispatch(first)
    assert receipt["result"]["execution_id"] == "execution-1"
    assert worker.dispatch(first) == receipt
    calls = list(database.calls)
    with pytest.raises(RuntimeConflict, match="conflict"):
        worker.dispatch(first | {"payload": {"execution_id": "execution-2"}})
    assert database.calls == calls


@pytest.mark.parametrize("execution_id", [None, True, 1, "", " padded ", "x" * 257])
def test_validation_rejects_missing_or_coerced_execution_owner(execution_id: Any) -> None:
    worker, request, database = fixture()
    calls = list(database.calls)
    with pytest.raises(ValueError, match="execution identity"):
        worker.dispatch(request | {"action": "VALIDATE", "payload": {"execution_id": execution_id}})
    assert database.calls == calls


def test_real_requests_materialize_parity_and_lost_ack_activation() -> None:
    worker, request, database = fixture()
    result = worker.dispatch(request)
    assert result["result"]["candidate_count"] == 1
    assert worker.dispatch(request) == result
    parity = worker.dispatch(request | {"action": "PARITY"})
    assert parity["result"]["decision"] == "ELIGIBLE"
    database.lose_ack = True
    activation = worker.dispatch(
        request
        | {
            "action": "ACTIVATE",
            "payload": {
                "expected_generation": None,
                "expected_version": 0,
                "parity_receipt_digest": parity["result"]["parity_digest"],
            },
        }
    )
    assert activation["result"]["pointer_version"] == 1
    assert sum(name == "TransactWriteItems" for name, _ in database.calls) == 3
    assert sum(name == "PutItem" for name, _ in database.calls) == 1


def test_corrupted_managed_records_fail_before_activation() -> None:
    worker, request, database = fixture()
    worker.dispatch(request)
    item = next(item for (table, _, _), item in database.items.items() if table == "online")
    item["value"] = {"N": "999"}
    with pytest.raises(RuntimeConflict, match="envelope"):
        worker.dispatch(request | {"action": "PARITY"})
    assert not any(
        name == "TransactWriteItems" and len(request["TransactItems"]) == 4
        for name, request in database.calls
    )


def test_repeated_scan_cursor_and_extra_record_fail_closed() -> None:
    worker, request, database = fixture()
    worker.dispatch(request)
    database.repeat_cursor = True
    with pytest.raises(RuntimeConflict, match="cursor repeated"):
        worker.dispatch(request | {"action": "PARITY"})


def test_parity_uses_source_not_caller_digests_and_freshness() -> None:
    worker, request, database = fixture()
    request["payload"]["request_time"] = 1011
    worker.dispatch(request)
    result = worker.dispatch(request | {"action": "PARITY"})["result"]
    assert result["decision"] == "QUARANTINED"
    assert "INELIGIBLE_RECORD" in result["parity_report"]["reasons"]


def test_runtime_rejects_conflicting_immutable_record() -> None:
    definition = FeatureDefinition(
        "count", 1, "integer", 1000, "transaction_count", 100, "count", 0
    )
    plan = build_materialization_plan(
        "risk",
        "op",
        (definition,),
        (FeatureValue("c1", "count", 1, 1000, 1000, definition.definition_digest, "g1"),),
        source_digest=digest({"source": 1}),
        materialized_at=1001,
    )
    database = DatabaseBoundary()
    runtime = DynamoOnlineRuntime(database, "online", "control")
    runtime.materialize(plan)
    row = next(item for (table, _, _), item in database.items.items() if table == "online")
    row["record_digest"] = {"S": "f" * 64}
    with pytest.raises(RuntimeConflict, match="envelope"):
        runtime.materialize(plan)
