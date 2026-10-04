"""Injected AWS boundaries and idempotent Stage 6 task authority.

No client is created at import time.  The pure request builders can be checked
against Botocore service models without sending an AWS request.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from featureforge.canonical import canonical_json, digest
from featureforge.managed import (
    ManagedContractError,
    S3ObjectAuthority,
    s3_get_object_request,
    verify_bytes,
)
from featureforge.online import (
    MaterializationPlan,
    activation_transaction_request,
    dynamodb_record_item,
    put_candidate_validation_request,
    put_record_request,
    validate_dynamodb_request,
)


class S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


class DynamoDBClient(Protocol):
    def get_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def update_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def transact_write_items(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def scan(self, **kwargs: Any) -> Mapping[str, Any]: ...


class GlueClient(Protocol):
    def start_job_run(self, **kwargs: Any) -> Mapping[str, Any]: ...


class RuntimeConflict(RuntimeError):
    """A durable identity was reused for different content."""


class TaskLedgerBoundary(Protocol):
    def begin(self, task_record: Mapping[str, Any]) -> dict[str, Any]: ...

    def complete(self, run_id: str, task: str, result: Mapping[str, Any]) -> dict[str, Any]: ...

    def get(self, run_id: str, task: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class ExactObject:
    authority: S3ObjectAuthority
    body: bytes


def read_exact_object(client: S3Client, authority: S3ObjectAuthority) -> ExactObject:
    response = client.get_object(**s3_get_object_request(authority))
    body_value = response.get("Body")
    if isinstance(body_value, bytes):
        body = body_value
    else:
        reader = getattr(body_value, "read", None)
        if not callable(reader):
            raise ManagedContractError("S3 GetObject body is not readable")
        body = reader()
    if not isinstance(body, bytes):
        raise ManagedContractError("S3 GetObject body is not bytes")
    returned_version = response.get("VersionId")
    if returned_version != authority.version_id:
        raise ManagedContractError("S3 returned a different object version")
    verify_bytes(body, authority.sha256)
    return ExactObject(authority=authority, body=body)


def put_json_object(client: S3Client, request: Mapping[str, Any]) -> S3ObjectAuthority:
    required = {
        "Body",
        "Bucket",
        "ExpectedBucketOwner",
        "Key",
        "ServerSideEncryption",
        "SSEKMSKeyId",
    }
    if not required.issubset(request):
        raise ManagedContractError("S3 PutObject request omits a security authority")
    body = request["Body"]
    if not isinstance(body, bytes):
        raise ManagedContractError("S3 PutObject body must be bytes")
    response = client.put_object(**dict(request))
    version_id = response.get("VersionId")
    if not isinstance(version_id, str) or not version_id:
        raise ManagedContractError("versioned S3 write did not return VersionId")
    checksum = response.get("ChecksumSHA256")
    expected = hashlib.sha256(body).hexdigest()
    expected_checksum = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
    if checksum is not None and checksum != expected_checksum:
        raise ManagedContractError("S3 response checksum disagrees with written bytes")
    return S3ObjectAuthority(
        bucket=str(request["Bucket"]),
        key=str(request["Key"]),
        version_id=version_id,
        sha256=expected,
    )


def task_key(run_id: str, task: str) -> dict[str, dict[str, str]]:
    return {"PK": {"S": f"RUN#{run_id}"}, "SK": {"S": f"TASK#{task}"}}


def task_get_request(table_name: str, run_id: str, task: str) -> dict[str, Any]:
    return {
        "ConsistentRead": True,
        "Key": task_key(run_id, task),
        "TableName": table_name,
    }


def task_put_request(table_name: str, task_record: Mapping[str, Any]) -> dict[str, Any]:
    run_id = str(task_record["run_id"])
    task = str(task_record["task"])
    item = task_key(run_id, task) | {
        "contract": {"S": str(task_record["contract"])},
        "manifest_digest": {"S": str(task_record["manifest_digest"])},
        "request_digest": {"S": str(task_record["request_digest"])},
        "state": {"S": str(task_record["state"])},
    }
    if task_record.get("result_digest") is not None:
        item["result_digest"] = {"S": str(task_record["result_digest"])}
    return {
        "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
        "Item": item,
        "TableName": table_name,
    }


def task_complete_request(
    table_name: str,
    current: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    result_value = dict(result)
    result_digest = digest(result_value)
    return {
        "ConditionExpression": (
            "#state = :started AND manifest_digest = :manifest AND request_digest = :request"
        ),
        "ExpressionAttributeNames": {"#state": "state"},
        "ExpressionAttributeValues": {
            ":completed": {"S": "COMPLETED"},
            ":manifest": {"S": str(current["manifest_digest"])},
            ":request": {"S": str(current["request_digest"])},
            ":result": {"S": result_digest},
            ":result_json": {"S": canonical_json(result_value)},
            ":started": {"S": "STARTED"},
        },
        "Key": task_key(str(current["run_id"]), str(current["task"])),
        "ReturnValues": "ALL_NEW",
        "TableName": table_name,
        "UpdateExpression": (
            "SET #state = :completed, result_digest = :result, result_json = :result_json"
        ),
    }


def _task_record(item: Any) -> dict[str, Any] | None:
    if item is None:
        return None
    if not isinstance(item, Mapping):
        raise RuntimeConflict("task ledger returned a malformed item")
    fields = ("contract", "manifest_digest", "request_digest", "state")
    result: dict[str, Any] = {}
    for field in fields:
        value = item.get(field)
        if not isinstance(value, Mapping) or not isinstance(value.get("S"), str):
            raise RuntimeConflict(f"task ledger item omits {field}")
        result[field] = value["S"]
    pk = item.get("PK")
    sk = item.get("SK")
    if not isinstance(pk, Mapping) or not isinstance(sk, Mapping):
        raise RuntimeConflict("task ledger item omits its key")
    pk_value = pk.get("S")
    sk_value = sk.get("S")
    if not isinstance(pk_value, str) or not pk_value.startswith("RUN#"):
        raise RuntimeConflict("task ledger partition key is malformed")
    if not isinstance(sk_value, str) or not sk_value.startswith("TASK#"):
        raise RuntimeConflict("task ledger sort key is malformed")
    result["run_id"] = pk_value.removeprefix("RUN#")
    result["task"] = sk_value.removeprefix("TASK#")
    result_value = item.get("result_digest")
    if result_value is not None:
        if not isinstance(result_value, Mapping) or not isinstance(result_value.get("S"), str):
            raise RuntimeConflict("task result digest is malformed")
        result["result_digest"] = result_value["S"]
        raw_json = item.get("result_json")
        if not isinstance(raw_json, Mapping) or not isinstance(raw_json.get("S"), str):
            raise RuntimeConflict("completed task result is absent")
        try:
            parsed = json.loads(raw_json["S"])
        except json.JSONDecodeError as error:
            raise RuntimeConflict("completed task result is malformed") from error
        if not isinstance(parsed, dict) or digest(parsed) != result["result_digest"]:
            raise RuntimeConflict("completed task result does not match its digest")
        result["result"] = parsed
    return result


def _error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return None
    detail = response.get("Error")
    if not isinstance(detail, Mapping):
        return None
    code = detail.get("Code")
    return code if isinstance(code, str) else None


class DynamoTaskLedger:
    """Persistent task authority using conditional DynamoDB writes and exact replay."""

    def __init__(self, client: DynamoDBClient, table_name: str) -> None:
        if not table_name:
            raise ManagedContractError("control table name is required")
        self._client = client
        self._table_name = table_name

    def _read(self, run_id: str, task: str) -> dict[str, Any] | None:
        response = self._client.get_item(**task_get_request(self._table_name, run_id, task))
        return _task_record(response.get("Item"))

    def get(self, run_id: str, task: str) -> dict[str, Any] | None:
        return self._read(run_id, task)

    @staticmethod
    def _assert_same(current: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
        for field in ("contract", "manifest_digest", "request_digest", "run_id", "task"):
            if current.get(field) != candidate.get(field):
                raise RuntimeConflict("task identity was reused with conflicting immutable content")

    def begin(self, task_record: Mapping[str, Any]) -> dict[str, Any]:
        run_id = str(task_record["run_id"])
        task = str(task_record["task"])
        current = self._read(run_id, task)
        if current is None:
            try:
                self._client.put_item(**task_put_request(self._table_name, task_record))
            except Exception as error:
                if _error_code(error) != "ConditionalCheckFailedException":
                    raise
                current = self._read(run_id, task)
                if current is None:
                    raise RuntimeConflict(
                        "conditional task conflict has no durable winner"
                    ) from error
            else:
                return dict(task_record)
        self._assert_same(current, task_record)
        return current

    def complete(self, run_id: str, task: str, result: Mapping[str, Any]) -> dict[str, Any]:
        current = self._read(run_id, task)
        if current is None:
            raise RuntimeConflict("task cannot complete before durable STARTED state")
        result_digest = digest(dict(result))
        if current.get("state") == "COMPLETED":
            if current.get("result_digest") != result_digest:
                raise RuntimeConflict("completed task replay returned different content")
            return current
        try:
            response = self._client.update_item(
                **task_complete_request(self._table_name, current, result)
            )
        except Exception as error:
            if _error_code(error) != "ConditionalCheckFailedException":
                raise
            replay = self._read(run_id, task)
            if replay is None or replay.get("state") != "COMPLETED":
                raise RuntimeConflict("task completion lost its conditional race") from error
            if replay.get("result_digest") != result_digest:
                raise RuntimeConflict("completed task replay returned different content") from error
            return replay
        updated = _task_record(response.get("Attributes"))
        if updated is None or updated.get("state") != "COMPLETED":
            raise RuntimeConflict("task completion returned no durable receipt")
        return updated


class TaskLedger:
    """Deterministic in-memory authority mirroring conditional DynamoDB semantics."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], dict[str, Any]] = {}

    def begin(self, task_record: Mapping[str, Any]) -> dict[str, Any]:
        key = (str(task_record["run_id"]), str(task_record["task"]))
        candidate = dict(task_record)
        current = self._records.get(key)
        if current is None:
            self._records[key] = candidate
            return dict(candidate)
        for field in ("contract", "manifest_digest", "request_digest"):
            if current.get(field) != candidate.get(field):
                raise RuntimeConflict("task identity was reused with conflicting immutable content")
        return dict(current)

    def complete(self, run_id: str, task: str, result: Mapping[str, Any]) -> dict[str, Any]:
        key = (run_id, task)
        current = self._records.get(key)
        if current is None:
            raise RuntimeConflict("task cannot complete before durable STARTED state")
        result_digest = digest(dict(result))
        if current.get("state") == "COMPLETED":
            if current.get("result_digest") != result_digest:
                raise RuntimeConflict("completed task replay returned different content")
            return dict(current)
        updated = current | {
            "result": dict(result),
            "result_digest": result_digest,
            "state": "COMPLETED",
        }
        self._records[key] = updated
        return dict(updated)

    def get(self, run_id: str, task: str) -> dict[str, Any] | None:
        current = self._records.get((run_id, task))
        return None if current is None else dict(current)


class DynamoOnlineRuntime:
    """Real conditional candidate writes, strong full-generation reads, and CAS.

    The bounded scan is deliberate: entity partitions cannot prove the absence
    of unexpected entities with per-entity queries alone. No AWS call occurs at
    construction or import time.
    """

    def __init__(self, client: DynamoDBClient, online_table: str, control_table: str) -> None:
        if not online_table or not control_table:
            raise ManagedContractError("online and control tables are required")
        self.client = client
        self.online_table = online_table
        self.control_table = control_table

    @staticmethod
    def _owner_key(plan: MaterializationPlan) -> dict[str, Any]:
        # Generation identity is global: feature_set is not present in online PK.
        return {"PK": {"S": f"GENERATION#{plan.generation_id}"}, "SK": {"S": "OWNER"}}

    def _owner(self, plan: MaterializationPlan) -> dict[str, Any] | None:
        value = self.client.get_item(
            TableName=self.control_table, Key=self._owner_key(plan), ConsistentRead=True
        ).get("Item")
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise RuntimeConflict("generation owner is malformed")
        for field, expected in (
            ("plan_digest", plan.plan_digest),
            ("feature_set", plan.feature_set),
            ("operation_id", plan.operation_id),
        ):
            if value.get(field) != {"S": expected}:
                raise RuntimeConflict("generation identity has a different immutable owner")
        if value.get("state") not in ({"S": "WRITING"}, {"S": "SEALED"}, {"S": "VALIDATED"}):
            raise RuntimeConflict("generation owner has an unknown lifecycle state")
        return dict(value)

    def _acquire(self, plan: MaterializationPlan) -> dict[str, Any]:
        current = self._owner(plan)
        if current is not None:
            return current
        item = self._owner_key(plan) | {
            "plan_digest": {"S": plan.plan_digest},
            "feature_set": {"S": plan.feature_set},
            "operation_id": {"S": plan.operation_id},
            "state": {"S": "WRITING"},
        }
        request = {
            "TableName": self.control_table,
            "Item": item,
            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
        }
        validate_dynamodb_request("PutItem", request)
        try:
            self.client.put_item(**request)
        except Exception:
            # Read after every uncertain response; only the exact durable owner
            # can establish that this acquisition committed.
            current = self._owner(plan)
            if current is None:
                raise
            return current
        return item

    def _condition(self, plan: MaterializationPlan, state: str) -> dict[str, Any]:
        return {
            "TableName": self.control_table,
            "Key": self._owner_key(plan),
            "ConditionExpression": "#state = :state AND plan_digest = :plan",
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": {":state": {"S": state}, ":plan": {"S": plan.plan_digest}},
        }

    def _write_record(self, plan: MaterializationPlan, record: Any) -> None:
        put = put_record_request(self.online_table, record)
        put.pop("ReturnValues")
        put["Item"]["record_json"] = {"S": canonical_json(record.as_dict())}
        request = {
            "TransactItems": [{"ConditionCheck": self._condition(plan, "WRITING")}, {"Put": put}],
            "ClientRequestToken": digest(
                {"plan": plan.plan_digest, "record": record.record_digest}
            )[:36],
        }
        validate_dynamodb_request("TransactWriteItems", request)
        self.client.transact_write_items(**request)

    def _seal(self, plan: MaterializationPlan) -> None:
        request = self._condition(plan, "WRITING") | {
            "UpdateExpression": "SET #state = :sealed",
            "ReturnValues": "ALL_NEW",
        }
        request["ExpressionAttributeValues"][":sealed"] = {"S": "SEALED"}
        validate_dynamodb_request("UpdateItem", request)
        try:
            self.client.update_item(**request)
        except Exception:
            current = self._owner(plan)
            if current is None or current["state"] not in ({"S": "SEALED"}, {"S": "VALIDATED"}):
                raise
        current = self._owner(plan)
        if current is None or current["state"] not in ({"S": "SEALED"}, {"S": "VALIDATED"}):
            raise RuntimeConflict("generation seal did not persist")

    def records(self, plan: MaterializationPlan) -> tuple[dict[str, Any], ...]:
        owner = self._owner(plan)
        if owner is None or owner["state"] == {"S": "WRITING"}:
            raise RuntimeConflict("exact reconciliation requires an immutable sealed generation")
        cursor: Mapping[str, Any] | None = None
        seen_cursors: set[str] = set()
        seen_keys: set[tuple[str, str]] = set()
        result: list[dict[str, Any]] = []
        for _ in range(plan.expected_count + 2):
            request: dict[str, Any] = {
                "TableName": self.online_table,
                "ConsistentRead": True,
                "Limit": 100,
                "FilterExpression": "generation_id = :generation AND feature_set = :feature_set",
                "ExpressionAttributeValues": {
                    ":generation": {"S": plan.generation_id},
                    ":feature_set": {"S": plan.feature_set},
                },
            }
            if cursor is not None:
                request["ExclusiveStartKey"] = dict(cursor)
            response = self.client.scan(**request)
            items = response.get("Items")
            if not isinstance(items, list):
                raise RuntimeConflict("candidate scan returned malformed items")
            for item in items:
                if not isinstance(item, Mapping):
                    raise RuntimeConflict("candidate item is malformed")
                raw = item.get("record_json")
                if not isinstance(raw, Mapping) or not isinstance(raw.get("S"), str):
                    raise RuntimeConflict("candidate record omits canonical content")
                row = json.loads(raw["S"])
                if not isinstance(row, dict):
                    raise RuntimeConflict("candidate record content is malformed")
                key = (str(row.get("partition_key")), str(row.get("sort_key")))
                if key in seen_keys:
                    raise RuntimeConflict("candidate scan repeated a record")
                seen_keys.add(key)
                result.append(row)
                if len(result) > plan.expected_count:
                    raise RuntimeConflict("candidate contains unexpected records")
                expected_record = next(
                    (
                        record
                        for record in plan.records
                        if (record.partition_key, record.sort_key) == key
                    ),
                    None,
                )
                if expected_record is None or row != expected_record.as_dict():
                    raise RuntimeConflict("candidate content differs from immutable plan")
                expected_item = dynamodb_record_item(expected_record) | {"record_json": raw}
                if dict(item) != expected_item:
                    raise RuntimeConflict("candidate DynamoDB envelope is corrupt")
            next_cursor = response.get("LastEvaluatedKey")
            if not next_cursor:
                ordered = tuple(
                    sorted(result, key=lambda row: (row["partition_key"], row["sort_key"]))
                )
                if ordered != tuple(record.as_dict() for record in plan.records):
                    raise RuntimeConflict("candidate is missing records")
                return ordered
            if not isinstance(next_cursor, Mapping):
                raise RuntimeConflict("candidate scan cursor is malformed")
            encoded = canonical_json(dict(next_cursor))
            if encoded in seen_cursors:
                raise RuntimeConflict("candidate scan cursor repeated")
            seen_cursors.add(encoded)
            cursor = next_cursor
        raise RuntimeConflict("candidate scan exceeded its bounded page budget")

    def materialize(self, plan: MaterializationPlan) -> dict[str, Any]:
        owner = self._acquire(plan)
        if owner["state"] == {"S": "WRITING"}:
            for record in plan.records:
                try:
                    self._write_record(plan, record)
                except Exception:
                    current = self._owner(plan)
                    if current is None or current["state"] == {"S": "WRITING"}:
                        raise
                    # A concurrent exact-plan writer sealed the generation. No
                    # further write is permitted; reconcile the now frozen set.
                    break
            self._seal(plan)
        # The strong scan is not a snapshot. The transaction condition above
        # prevents all admitted writers from changing membership after sealing,
        # so every page now observes one immutable generation.
        actual = self.records(plan)
        body: dict[str, Any] = {
            "actual_count": len(actual),
            "actual_digest": plan.records_digest,
            "contract": "online-validation-receipt-v1",
            "definition_set_digest": plan.definition_set_digest,
            "feature_set": plan.feature_set,
            "generation_id": plan.generation_id,
            "offline_rows_digest": plan.offline_rows_digest,
            "operation_id": plan.operation_id,
            "plan_digest": plan.plan_digest,
            "source_digest": plan.source_digest,
        }
        receipt = body | {"receipt_digest": digest(body)}
        if self._owner(plan) is None:
            raise RuntimeConflict("sealed generation owner disappeared")
        candidate = put_candidate_validation_request(self.control_table, plan, receipt)
        candidate.pop("ReturnValues")
        update = self._condition(plan, "SEALED") | {
            "UpdateExpression": "SET #state = :validated, validation_receipt_digest = :receipt"
        }
        update["ExpressionAttributeValues"] |= {
            ":validated": {"S": "VALIDATED"},
            ":receipt": {"S": receipt["receipt_digest"]},
        }
        request = {
            "TransactItems": [{"Update": update}, {"Put": candidate}],
            "ClientRequestToken": digest(
                {"plan": plan.plan_digest, "validation": receipt["receipt_digest"]}
            )[:36],
        }
        validate_dynamodb_request("TransactWriteItems", request)
        try:
            self.client.transact_write_items(**request)
        except Exception:
            current = self._owner(plan)
            if (
                current is None
                or current["state"] != {"S": "VALIDATED"}
                or current.get("validation_receipt_digest") != {"S": receipt["receipt_digest"]}
            ):
                raise
        control_key = {
            "PK": {"S": f"CONTROL#{plan.feature_set}"},
            "SK": {"S": f"GEN#{plan.generation_id}"},
        }
        persisted = self.client.get_item(
            TableName=self.control_table, Key=control_key, ConsistentRead=True
        ).get("Item")
        if persisted != candidate["Item"]:
            raise RuntimeConflict("validation transaction has no exact durable candidate receipt")
        return receipt

    def activate(
        self,
        *,
        plan: MaterializationPlan,
        validation_receipt_digest: str,
        expected_generation: str | None,
        expected_version: int,
        operation_id: str,
    ) -> dict[str, Any]:
        request = activation_transaction_request(
            self.control_table,
            feature_set=plan.feature_set,
            generation_id=plan.generation_id,
            validation_receipt_digest=validation_receipt_digest,
            expected_generation=expected_generation,
            expected_version=expected_version,
            operation_id=operation_id,
            actor="stage6-control-worker",
            reason="independently-verified-candidate-promotion",
        )
        seal_check = self._condition(plan, "VALIDATED")
        seal_check["ConditionExpression"] += " AND validation_receipt_digest = :receipt"
        seal_check["ExpressionAttributeValues"][":receipt"] = {"S": validation_receipt_digest}
        request["TransactItems"].append({"ConditionCheck": seal_check})
        validate_dynamodb_request("TransactWriteItems", request)
        operation_key = {
            "PK": {"S": f"CONTROL#{plan.feature_set}"},
            "SK": {"S": f"OP#{operation_id}"},
        }
        expected_operation = request["TransactItems"][2]["Put"]["Item"]

        def replay() -> bool:
            value = self.client.get_item(
                TableName=self.control_table, Key=operation_key, ConsistentRead=True
            ).get("Item")
            if value is None:
                return False
            if value != expected_operation:
                raise RuntimeConflict("activation operation was reused with different authority")
            return True

        if not replay():
            try:
                self.client.transact_write_items(**request)
            except Exception:
                # Confirm only the exact durable operation, including after a
                # transport error. Absence is never treated as success.
                if not replay():
                    raise
        body = {
            "feature_set": plan.feature_set,
            "generation_id": plan.generation_id,
            "operation_id": operation_id,
            "pointer_version": expected_version + 1,
            "validation_receipt_digest": validation_receipt_digest,
        }
        return body | {"activation_digest": digest(body)}


def canonical_payload_bytes(value: Mapping[str, Any]) -> bytes:
    return canonical_json(dict(value)).encode("utf-8")
