"""Injected AWS boundaries and idempotent Stage 6 task authority.

No client is created at import time.  The pure request builders can be checked
against Botocore service models without sending an AWS request.
"""

from __future__ import annotations

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


class S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


class DynamoDBClient(Protocol):
    def get_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def update_item(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def transact_write_items(self, **kwargs: Any) -> Mapping[str, Any]: ...


class GlueClient(Protocol):
    def start_job_run(self, **kwargs: Any) -> Mapping[str, Any]: ...


class RuntimeConflict(RuntimeError):
    """A durable identity was reused for different content."""


class TaskLedgerBoundary(Protocol):
    def begin(self, task_record: Mapping[str, Any]) -> dict[str, Any]: ...

    def complete(
        self, run_id: str, task: str, result: Mapping[str, Any]
    ) -> dict[str, Any]: ...


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
    if checksum is not None and checksum != expected:
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
            "#state = :started AND manifest_digest = :manifest "
            "AND request_digest = :request"
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
            "SET #state = :completed, result_digest = :result, "
            "result_json = :result_json"
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
        response = self._client.get_item(
            **task_get_request(self._table_name, run_id, task)
        )
        return _task_record(response.get("Item"))

    @staticmethod
    def _assert_same(current: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
        for field in ("contract", "manifest_digest", "request_digest", "run_id", "task"):
            if current.get(field) != candidate.get(field):
                raise RuntimeConflict(
                    "task identity was reused with conflicting immutable content"
                )

    def begin(self, task_record: Mapping[str, Any]) -> dict[str, Any]:
        run_id = str(task_record["run_id"])
        task = str(task_record["task"])
        current = self._read(run_id, task)
        if current is None:
            try:
                self._client.put_item(
                    **task_put_request(self._table_name, task_record)
                )
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

    def complete(
        self, run_id: str, task: str, result: Mapping[str, Any]
    ) -> dict[str, Any]:
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


def canonical_payload_bytes(value: Mapping[str, Any]) -> bytes:
    return canonical_json(dict(value)).encode("utf-8")
