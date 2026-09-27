"""DynamoDB-shaped online materialization and serving contract.

The request boundary emits production-valid DynamoDB low-level request shapes.
The SQLite authority provides deterministic local evidence for the same modeled
outcomes; it is not evidence of live DynamoDB execution.
"""

from __future__ import annotations

import json
import math
import random
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from featureforge.canonical import canonical_json, digest
from featureforge.model import FeatureDefinition, FeatureValue

ONLINE_RECORD_CONTRACT = "online-feature-record-v1"
MATERIALIZATION_PLAN_CONTRACT = "online-materialization-plan-v1"
VALIDATION_RECEIPT_CONTRACT = "online-validation-receipt-v1"
ACTIVATION_RECEIPT_CONTRACT = "online-activation-receipt-v1"
SERVING_RESULT_CONTRACT = "online-serving-result-v1"
MAX_DYNAMODB_ITEM_BYTES = 400_000
SCHEMA_VERSION = 1


class OnlineContractError(ValueError):
    """Input cannot be represented without weakening the online contract."""


class OnlineStateError(RuntimeError):
    """Persistent online state violates a required transition."""


class OnlineConflict(OnlineStateError):
    """An identity or key was reused with different immutable content."""


class CandidateValidationError(OnlineStateError):
    """A candidate is incomplete, extra, corrupt, or otherwise invalid."""


class ActivationConflict(OnlineStateError):
    """The candidate or active pointer no longer matches the request."""


class MaterializationInterrupted(OnlineStateError):
    """Deterministic fault injected after a durable record write."""


class AcknowledgementLost(OnlineStateError):
    """Activation committed, but its response was deliberately dropped."""


ValueKind = Literal["integer", "float", "null"]
ServingStatus = Literal[
    "FOUND",
    "ABSENT",
    "EXPIRED",
    "STALE",
    "DEFINITION_MISMATCH",
    "GENERATION_UNAVAILABLE",
    "CORRUPT_RECORD",
    "STORAGE_FAILURE",
    "THROTTLED",
]
ErrorClass = Literal["RETRYABLE", "CONFLICT", "TERMINAL", "UNKNOWN"]


def _canonical_decimal(value: int | float) -> str:
    if isinstance(value, bool):
        raise OnlineContractError("boolean values are not feature numbers")
    if isinstance(value, float) and not math.isfinite(value):
        raise OnlineContractError("nonfinite feature values are unsupported")
    number = Decimal(str(value))
    if not number.is_finite():
        raise OnlineContractError("nonfinite feature values are unsupported")
    if number == 0:
        return "0"
    rendered = format(number.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _encode_value(
    definition: FeatureDefinition, value: int | float | None
) -> dict[str, Any]:
    if value is None:
        return {"kind": "null"}
    if definition.value_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise OnlineContractError("integer definition received a non-integer value")
        return {"integer": value, "kind": "integer"}
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OnlineContractError("float definition received a non-numeric value")
    return {"decimal": _canonical_decimal(value), "kind": "float"}


def _decode_value(value: dict[str, Any]) -> int | float | None:
    kind = value.get("kind")
    if kind == "null" and set(value) == {"kind"}:
        return None
    if kind == "integer" and set(value) == {"integer", "kind"}:
        integer = value["integer"]
        if isinstance(integer, bool) or not isinstance(integer, int):
            raise OnlineContractError("stored integer value is invalid")
        return integer
    if kind == "float" and set(value) == {"decimal", "kind"}:
        decimal = value["decimal"]
        if not isinstance(decimal, str):
            raise OnlineContractError("stored decimal value is invalid")
        parsed = float(Decimal(decimal))
        if not math.isfinite(parsed):
            raise OnlineContractError("stored decimal value is nonfinite")
        return parsed
    raise OnlineContractError("stored typed value has an unknown shape")


@dataclass(frozen=True)
class OnlineRecord:
    feature_set: str
    generation_id: str
    customer_id: str
    feature_name: str
    definition_digest: str
    value: dict[str, Any]
    event_cutoff: int
    knowledge_cutoff: int
    source_digest: str
    offline_rows_digest: str
    operation_id: str
    materialized_at: int
    expires_at: int
    record_digest: str

    @property
    def partition_key(self) -> str:
        return f"ENTITY#{self.customer_id}#GEN#{self.generation_id}"

    @property
    def sort_key(self) -> str:
        return f"FEATURE#{self.feature_name}#DEF#{self.definition_digest}"

    def body(self) -> dict[str, Any]:
        return {
            "contract": ONLINE_RECORD_CONTRACT,
            "customer_id": self.customer_id,
            "definition_digest": self.definition_digest,
            "event_cutoff": self.event_cutoff,
            "expires_at": self.expires_at,
            "feature_name": self.feature_name,
            "feature_set": self.feature_set,
            "generation_id": self.generation_id,
            "knowledge_cutoff": self.knowledge_cutoff,
            "materialized_at": self.materialized_at,
            "offline_rows_digest": self.offline_rows_digest,
            "operation_id": self.operation_id,
            "partition_key": self.partition_key,
            "sort_key": self.sort_key,
            "source_digest": self.source_digest,
            "ttl_epoch_seconds": self.expires_at,
            "value": self.value,
        }

    def as_dict(self) -> dict[str, Any]:
        return self.body() | {"record_digest": self.record_digest}

    def verified(self) -> bool:
        return self.record_digest == digest(self.body())


@dataclass(frozen=True)
class MaterializationPlan:
    feature_set: str
    generation_id: str
    operation_id: str
    source_digest: str
    offline_rows_digest: str
    definition_set_digest: str
    records: tuple[OnlineRecord, ...]
    records_digest: str
    plan_digest: str

    @property
    def expected_count(self) -> int:
        return len(self.records)

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": MATERIALIZATION_PLAN_CONTRACT,
            "definition_set_digest": self.definition_set_digest,
            "expected_count": self.expected_count,
            "feature_set": self.feature_set,
            "generation_id": self.generation_id,
            "offline_rows_digest": self.offline_rows_digest,
            "operation_id": self.operation_id,
            "plan_digest": self.plan_digest,
            "records_digest": self.records_digest,
            "source_digest": self.source_digest,
        }


@dataclass(frozen=True)
class ReaderToken:
    feature_set: str
    generation_id: str
    pointer_version: int


@dataclass(frozen=True)
class ServingResult:
    status: ServingStatus
    feature_set: str
    generation_id: str
    customer_id: str
    feature_name: str
    definition_digest: str | None = None
    record_digest: str | None = None
    value: int | float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"contract": SERVING_RESULT_CONTRACT, **asdict(self)}


def build_materialization_plan(
    feature_set: str,
    operation_id: str,
    definitions: Sequence[FeatureDefinition],
    values: Sequence[FeatureValue],
    *,
    source_digest: str,
    materialized_at: int,
) -> MaterializationPlan:
    if not feature_set or not operation_id or not source_digest:
        raise OnlineContractError("feature set, operation, and source identities are required")
    if materialized_at < 0:
        raise OnlineContractError("materialization time must be non-negative")
    if not definitions or not values:
        raise OnlineContractError("definitions and offline values must be non-empty")
    definition_by_digest = {
        definition.definition_digest: definition for definition in definitions
    }
    if len(definition_by_digest) != len(definitions):
        raise OnlineContractError("definition digest collision")
    ordered_values = tuple(
        sorted(values, key=lambda item: (item.customer_id, item.feature_name))
    )
    if len({(value.customer_id, value.feature_name) for value in ordered_values}) != len(
        ordered_values
    ):
        raise OnlineContractError("duplicate entity-feature key in offline generation")
    generation_ids = {value.generation_id for value in ordered_values}
    if len(generation_ids) != 1:
        raise OnlineContractError("online materialization requires exactly one generation")
    generation_id = next(iter(generation_ids))
    offline_rows = tuple(value.as_dict() for value in ordered_values)
    offline_rows_digest = digest(offline_rows)
    definition_set_digest = digest(
        sorted(definition.definition_digest for definition in definitions)
    )
    records: list[OnlineRecord] = []
    for value in ordered_values:
        definition = definition_by_digest.get(value.definition_digest)
        if definition is None or definition.name != value.feature_name:
            raise OnlineContractError(
                "feature value references an unknown or mismatched definition"
            )
        body = {
            "contract": ONLINE_RECORD_CONTRACT,
            "customer_id": value.customer_id,
            "definition_digest": value.definition_digest,
            "event_cutoff": value.event_time,
            "expires_at": value.event_time + definition.ttl_seconds,
            "feature_name": value.feature_name,
            "feature_set": feature_set,
            "generation_id": generation_id,
            "knowledge_cutoff": value.knowledge_time,
            "materialized_at": materialized_at,
            "offline_rows_digest": offline_rows_digest,
            "operation_id": operation_id,
            "partition_key": f"ENTITY#{value.customer_id}#GEN#{generation_id}",
            "sort_key": f"FEATURE#{value.feature_name}#DEF#{value.definition_digest}",
            "source_digest": source_digest,
            "ttl_epoch_seconds": value.event_time + definition.ttl_seconds,
            "value": _encode_value(definition, value.value),
        }
        records.append(
            OnlineRecord(
                feature_set=feature_set,
                generation_id=generation_id,
                customer_id=value.customer_id,
                feature_name=value.feature_name,
                definition_digest=value.definition_digest,
                value=cast(dict[str, Any], body["value"]),
                event_cutoff=value.event_time,
                knowledge_cutoff=value.knowledge_time,
                source_digest=source_digest,
                offline_rows_digest=offline_rows_digest,
                operation_id=operation_id,
                materialized_at=materialized_at,
                expires_at=value.event_time + definition.ttl_seconds,
                record_digest=digest(body),
            )
        )
    ordered_records = tuple(
        sorted(records, key=lambda item: (item.partition_key, item.sort_key))
    )
    records_digest = digest([record.record_digest for record in ordered_records])
    plan_body = {
        "contract": MATERIALIZATION_PLAN_CONTRACT,
        "definition_set_digest": definition_set_digest,
        "expected_count": len(ordered_records),
        "feature_set": feature_set,
        "generation_id": generation_id,
        "offline_rows_digest": offline_rows_digest,
        "operation_id": operation_id,
        "records_digest": records_digest,
        "source_digest": source_digest,
    }
    return MaterializationPlan(
        feature_set=feature_set,
        generation_id=generation_id,
        operation_id=operation_id,
        source_digest=source_digest,
        offline_rows_digest=offline_rows_digest,
        definition_set_digest=definition_set_digest,
        records=ordered_records,
        records_digest=records_digest,
        plan_digest=digest(plan_body),
    )


def _s(value: str) -> dict[str, str]:
    return {"S": value}


def _n(value: int | str) -> dict[str, str]:
    return {"N": str(value)}


def dynamodb_record_item(record: OnlineRecord) -> dict[str, dict[str, Any]]:
    if not record.verified():
        raise OnlineContractError("record digest does not bind its content")
    item: dict[str, dict[str, Any]] = {
        "PK": _s(record.partition_key),
        "SK": _s(record.sort_key),
        "contract": _s(ONLINE_RECORD_CONTRACT),
        "customer_id": _s(record.customer_id),
        "definition_digest": _s(record.definition_digest),
        "event_cutoff": _n(record.event_cutoff),
        "expires_at": _n(record.expires_at),
        "feature_name": _s(record.feature_name),
        "feature_set": _s(record.feature_set),
        "generation_id": _s(record.generation_id),
        "knowledge_cutoff": _n(record.knowledge_cutoff),
        "materialized_at": _n(record.materialized_at),
        "offline_rows_digest": _s(record.offline_rows_digest),
        "operation_id": _s(record.operation_id),
        "record_digest": _s(record.record_digest),
        "source_digest": _s(record.source_digest),
        "ttl_epoch_seconds": _n(record.expires_at),
        "value_kind": _s(str(record.value["kind"])),
    }
    if record.value["kind"] == "integer":
        item["value"] = _n(cast(int, record.value["integer"]))
    elif record.value["kind"] == "float":
        item["value"] = _n(cast(str, record.value["decimal"]))
    elif record.value["kind"] == "null":
        item["value"] = {"NULL": True}
    else:
        raise OnlineContractError("unsupported typed online value")
    return item


def put_record_request(table_name: str, record: OnlineRecord) -> dict[str, Any]:
    if not table_name:
        raise OnlineContractError("table name is required")
    return {
        "TableName": table_name,
        "Item": dynamodb_record_item(record),
        "ConditionExpression": (
            "(attribute_not_exists(PK) AND attribute_not_exists(SK)) "
            "OR record_digest = :record_digest"
        ),
        "ExpressionAttributeValues": {
            ":record_digest": _s(record.record_digest),
        },
        "ReturnValues": "NONE",
    }


def query_generation_request(
    table_name: str,
    token: ReaderToken,
    customer_id: str,
    *,
    exclusive_start_key: dict[str, dict[str, Any]] | None = None,
    page_size: int = 100,
) -> dict[str, Any]:
    if not table_name or not customer_id or page_size < 1:
        raise OnlineContractError("table, customer, and positive page size are required")
    request: dict[str, Any] = {
        "TableName": table_name,
        "ConsistentRead": True,
        "Limit": page_size,
        "KeyConditionExpression": "PK = :pk",
        "ExpressionAttributeValues": {
            ":pk": _s(f"ENTITY#{customer_id}#GEN#{token.generation_id}")
        },
    }
    if exclusive_start_key is not None:
        request["ExclusiveStartKey"] = exclusive_start_key
    return request


def put_candidate_validation_request(
    table_name: str,
    plan: MaterializationPlan,
    receipt: dict[str, Any],
) -> dict[str, Any]:
    """Build the immutable control-item write consumed by activation."""

    if not table_name:
        raise OnlineContractError("table name is required")
    receipt_digest = receipt.get("receipt_digest")
    if (
        receipt.get("generation_id") != plan.generation_id
        or receipt.get("plan_digest") != plan.plan_digest
        or not isinstance(receipt_digest, str)
    ):
        raise OnlineContractError("validation receipt does not bind the plan")
    return {
        "TableName": table_name,
        "Item": {
            "PK": _s(f"CONTROL#{plan.feature_set}"),
            "SK": _s(f"GEN#{plan.generation_id}"),
            "kind": _s("CANDIDATE"),
            "state": _s("VALIDATED"),
            "generation_id": _s(plan.generation_id),
            "plan_digest": _s(plan.plan_digest),
            "records_digest": _s(plan.records_digest),
            "validation_receipt_digest": _s(receipt_digest),
        },
        "ConditionExpression": (
            "(attribute_not_exists(PK) AND attribute_not_exists(SK)) "
            "OR (plan_digest = :plan AND validation_receipt_digest = :receipt)"
        ),
        "ExpressionAttributeValues": {
            ":plan": _s(plan.plan_digest),
            ":receipt": _s(receipt_digest),
        },
        "ReturnValues": "NONE",
    }


def activation_transaction_request(
    table_name: str,
    *,
    feature_set: str,
    generation_id: str,
    validation_receipt_digest: str,
    expected_generation: str | None,
    expected_version: int,
    operation_id: str,
    actor: str = "stage4-local-proof",
    reason: str = "validated-candidate-promotion",
) -> dict[str, Any]:
    if (
        not table_name
        or not feature_set
        or not generation_id
        or not validation_receipt_digest
        or not operation_id
        or not actor
        or not reason
        or len(actor) > 128
        or len(reason) > 256
        or expected_version < 0
    ):
        raise OnlineContractError("activation request identity is incomplete")
    request_body = {
        "expected_generation": expected_generation,
        "expected_version": expected_version,
        "feature_set": feature_set,
        "generation_id": generation_id,
        "operation_id": operation_id,
        "actor": actor,
        "reason": reason,
        "validation_receipt_digest": validation_receipt_digest,
    }
    request_digest = digest(request_body)
    control_pk = f"CONTROL#{feature_set}"
    pointer_condition = (
        "attribute_not_exists(active_generation) AND "
        "attribute_not_exists(pointer_version)"
        if expected_generation is None and expected_version == 0
        else (
            "active_generation = :expected_generation "
            "AND pointer_version = :expected_version"
        )
    )
    pointer_values = {
        ":generation": _s(generation_id),
        ":new_version": _n(expected_version + 1),
    }
    if expected_generation is not None:
        pointer_values[":expected_generation"] = _s(expected_generation)
        pointer_values[":expected_version"] = _n(expected_version)
    return {
        "ClientRequestToken": request_digest[:36],
        "TransactItems": [
            {
                "ConditionCheck": {
                    "TableName": table_name,
                    "Key": {"PK": _s(control_pk), "SK": _s(f"GEN#{generation_id}")},
                    "ConditionExpression": (
                        "#state = :validated AND validation_receipt_digest = :receipt"
                    ),
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":receipt": _s(validation_receipt_digest),
                        ":validated": _s("VALIDATED"),
                    },
                }
            },
            {
                "Update": {
                    "TableName": table_name,
                    "Key": {"PK": _s(control_pk), "SK": _s("ACTIVE")},
                    "UpdateExpression": (
                        "SET active_generation = :generation, "
                        "pointer_version = :new_version"
                    ),
                    "ConditionExpression": pointer_condition,
                    "ExpressionAttributeValues": pointer_values,
                }
            },
            {
                "Put": {
                    "TableName": table_name,
                    "Item": {
                        "PK": _s(control_pk),
                        "SK": _s(f"OP#{operation_id}"),
                        "kind": _s("ACTIVATE"),
                        "request_digest": _s(request_digest),
                        "generation_id": _s(generation_id),
                        "actor": _s(actor),
                        "reason": _s(reason),
                        "pointer_version": _n(expected_version + 1),
                    },
                    "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                }
            },
        ],
    }


def validate_dynamodb_request(operation_name: str, request: dict[str, Any]) -> None:
    """Validate a low-level request against Botocore's pinned service model."""

    from botocore.session import Session  # type: ignore[import-untyped]
    from botocore.validate import ParamValidator  # type: ignore[import-untyped]

    service = Session().get_service_model("dynamodb")
    operation = service.operation_model(operation_name)
    report = ParamValidator().validate(request, operation.input_shape)
    if report.has_errors():
        raise OnlineContractError(report.generate_report())


def classify_dynamodb_error(code: str) -> ErrorClass:
    """Map stable DynamoDB error codes without treating unknown errors as retryable."""

    retryable = {
        "InternalServerError",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "ThrottlingException",
        "TransactionInProgressException",
    }
    conflicts = {"ConditionalCheckFailedException", "TransactionCanceledException"}
    terminal = {
        "AccessDeniedException",
        "ResourceNotFoundException",
        "ValidationException",
    }
    if code in retryable:
        return "RETRYABLE"
    if code in conflicts:
        return "CONFLICT"
    if code in terminal:
        return "TERMINAL"
    return "UNKNOWN"


class DynamoDBClient(Protocol):
    def put_item(self, **request: Any) -> dict[str, Any]: ...

    def query(self, **request: Any) -> dict[str, Any]: ...

    def transact_write_items(self, **request: Any) -> dict[str, Any]: ...


class DynamoDBOnlineBoundary:
    """Production-call boundary with validated request shapes and injected client."""

    def __init__(self, table_name: str, client: DynamoDBClient) -> None:
        if not table_name:
            raise OnlineContractError("table name is required")
        self.table_name = table_name
        self.client = client

    def put_record(self, record: OnlineRecord) -> dict[str, Any]:
        request = put_record_request(self.table_name, record)
        validate_dynamodb_request("PutItem", request)
        return self.client.put_item(**request)

    def query_generation(
        self,
        token: ReaderToken,
        customer_id: str,
        *,
        exclusive_start_key: dict[str, dict[str, Any]] | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        request = query_generation_request(
            self.table_name,
            token,
            customer_id,
            exclusive_start_key=exclusive_start_key,
            page_size=page_size,
        )
        validate_dynamodb_request("Query", request)
        return self.client.query(**request)

    def put_candidate_validation(
        self, plan: MaterializationPlan, receipt: dict[str, Any]
    ) -> dict[str, Any]:
        request = put_candidate_validation_request(self.table_name, plan, receipt)
        validate_dynamodb_request("PutItem", request)
        return self.client.put_item(**request)

    def activate(
        self,
        *,
        feature_set: str,
        generation_id: str,
        validation_receipt_digest: str,
        expected_generation: str | None,
        expected_version: int,
        operation_id: str,
        actor: str = "stage4-local-proof",
        reason: str = "validated-candidate-promotion",
    ) -> dict[str, Any]:
        request = activation_transaction_request(
            self.table_name,
            feature_set=feature_set,
            generation_id=generation_id,
            validation_receipt_digest=validation_receipt_digest,
            expected_generation=expected_generation,
            expected_version=expected_version,
            operation_id=operation_id,
            actor=actor,
            reason=reason,
        )
        validate_dynamodb_request("TransactWriteItems", request)
        return self.client.transact_write_items(**request)


SCHEMA = f"""
CREATE TABLE candidates (
    generation_id TEXT PRIMARY KEY,
    feature_set TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('WRITING','VALIDATED','ACTIVE','RETIRED','FAILED')),
    plan_digest TEXT NOT NULL,
    expected_count INTEGER NOT NULL,
    expected_digest TEXT NOT NULL,
    receipt_json TEXT,
    receipt_digest TEXT
);
CREATE TABLE records (
    generation_id TEXT NOT NULL REFERENCES candidates(generation_id),
    customer_id TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    record_json TEXT NOT NULL,
    record_digest TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY(generation_id, customer_id, feature_name)
);
CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('MATERIALIZE','ACTIVATE')),
    request_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('WRITING','COMPLETED','FAILED')),
    result_json TEXT
);
CREATE TABLE retry_attempts (
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    attempt INTEGER NOT NULL CHECK(attempt >= 1),
    outcome TEXT NOT NULL CHECK(outcome IN ('RETRYABLE','CONFIRMED','TERMINAL')),
    confirmed_count INTEGER NOT NULL CHECK(confirmed_count >= 0),
    next_retry_at INTEGER,
    terminal_reason TEXT,
    attempt_digest TEXT NOT NULL,
    PRIMARY KEY(operation_id, attempt)
);
CREATE TABLE pointers (
    feature_set TEXT PRIMARY KEY,
    generation_id TEXT,
    pointer_version INTEGER NOT NULL
);
CREATE TABLE pointer_history (
    feature_set TEXT NOT NULL,
    pointer_version INTEGER NOT NULL,
    from_generation TEXT,
    to_generation TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    receipt_digest TEXT NOT NULL,
    PRIMARY KEY(feature_set, pointer_version)
);
PRAGMA user_version = {SCHEMA_VERSION};
"""


class LocalOnlineStore:
    """Durable SQLite adapter for the contract's modeled observable behavior."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self.connection = sqlite3.connect(
            str(self.database), isolation_level=None, timeout=5.0
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if version == 0:
            self.connection.executescript(SCHEMA)
        elif version != SCHEMA_VERSION:
            self.connection.close()
            raise OnlineStateError(
                f"unsupported online schema version {version}; expected {SCHEMA_VERSION}"
            )

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def _operation(
        self, operation_id: str, kind: str, request_digest: str
    ) -> sqlite3.Row | None:
        existing = self.connection.execute(
            "SELECT * FROM operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if existing is not None and (
            existing["kind"] != kind or existing["request_digest"] != request_digest
        ):
            raise OnlineConflict("operation identity was reused with different content")
        return cast(sqlite3.Row | None, existing)

    def begin_materialization(self, plan: MaterializationPlan) -> dict[str, Any] | None:
        with self._transaction():
            existing = self._operation(
                plan.operation_id, "MATERIALIZE", plan.plan_digest
            )
            if existing is not None and existing["state"] == "COMPLETED":
                return cast(dict[str, Any], json.loads(existing["result_json"]))
            candidate = self.connection.execute(
                "SELECT plan_digest FROM candidates WHERE generation_id=?",
                (plan.generation_id,),
            ).fetchone()
            if candidate is not None and candidate["plan_digest"] != plan.plan_digest:
                raise OnlineConflict("generation identity was reused with another plan")
            if existing is None:
                self.connection.execute(
                    "INSERT INTO operations VALUES (?, 'MATERIALIZE', ?, 'WRITING', NULL)",
                    (plan.operation_id, plan.plan_digest),
                )
            if candidate is None:
                self.connection.execute(
                    """INSERT INTO candidates
                       VALUES (?, ?, 'WRITING', ?, ?, ?, NULL, NULL)""",
                    (
                        plan.generation_id,
                        plan.feature_set,
                        plan.plan_digest,
                        plan.expected_count,
                        plan.records_digest,
                    ),
                )
            self.connection.execute(
                "INSERT OR IGNORE INTO pointers VALUES (?, NULL, 0)",
                (plan.feature_set,),
            )
        return None

    def write_record(self, plan: MaterializationPlan, record: OnlineRecord) -> str:
        if record not in plan.records or not record.verified():
            raise OnlineContractError("record is not an intact member of the plan")
        with self._transaction():
            candidate = self.connection.execute(
                "SELECT state, plan_digest FROM candidates WHERE generation_id=?",
                (plan.generation_id,),
            ).fetchone()
            if candidate is None or candidate["plan_digest"] != plan.plan_digest:
                raise OnlineStateError("materialization plan was not admitted")
            if candidate["state"] not in {"WRITING", "VALIDATED"}:
                raise OnlineStateError("candidate is not writable")
            existing = self.connection.execute(
                """SELECT record_digest FROM records
                   WHERE generation_id=? AND customer_id=? AND feature_name=?""",
                (record.generation_id, record.customer_id, record.feature_name),
            ).fetchone()
            if existing is not None:
                if existing["record_digest"] != record.record_digest:
                    raise OnlineConflict("online record key has conflicting content")
                return "replayed"
            if candidate["state"] == "VALIDATED":
                raise OnlineStateError("validated candidate cannot accept new records")
            self.connection.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    record.generation_id,
                    record.customer_id,
                    record.feature_name,
                    record.definition_digest,
                    canonical_json(record.as_dict()),
                    record.record_digest,
                    record.expires_at,
                ),
            )
        return "written"

    def missing_records(self, plan: MaterializationPlan) -> tuple[OnlineRecord, ...]:
        stored = {
            (row["customer_id"], row["feature_name"])
            for row in self.connection.execute(
                "SELECT customer_id, feature_name FROM records WHERE generation_id=?",
                (plan.generation_id,),
            )
        }
        return tuple(
            record
            for record in plan.records
            if (record.customer_id, record.feature_name) not in stored
        )

    def candidate_page(
        self,
        generation_id: str,
        *,
        page_size: int,
        after: tuple[str, str] | None = None,
    ) -> tuple[tuple[dict[str, Any], ...], tuple[str, str] | None]:
        """Return a deterministic keyset page and the next exclusive cursor."""

        if page_size < 1:
            raise OnlineContractError("page size must be positive")
        if after is None:
            rows = self.connection.execute(
                """SELECT customer_id, feature_name, record_json, record_digest
                   FROM records WHERE generation_id=?
                   ORDER BY customer_id, feature_name LIMIT ?""",
                (generation_id, page_size + 1),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """SELECT customer_id, feature_name, record_json, record_digest
                   FROM records WHERE generation_id=? AND
                   (customer_id > ? OR (customer_id = ? AND feature_name > ?))
                   ORDER BY customer_id, feature_name LIMIT ?""",
                (generation_id, after[0], after[0], after[1], page_size + 1),
            ).fetchall()
        page_rows = rows[:page_size]
        next_cursor = None
        if len(rows) > page_size:
            final = page_rows[-1]
            next_cursor = (str(final["customer_id"]), str(final["feature_name"]))
        return tuple(dict(row) for row in page_rows), next_cursor

    def candidate_records(
        self, generation_id: str, *, page_size: int = 100
    ) -> tuple[dict[str, Any], ...]:
        """Traverse every stable keyset page without offset-based ambiguity."""

        result: list[dict[str, Any]] = []
        cursor: tuple[str, str] | None = None
        while True:
            page, cursor = self.candidate_page(
                generation_id, page_size=page_size, after=cursor
            )
            result.extend(page)
            if cursor is None:
                return tuple(result)

    def reconcile(self, plan: MaterializationPlan) -> dict[str, Any]:
        with self._transaction():
            candidate = self.connection.execute(
                "SELECT * FROM candidates WHERE generation_id=?",
                (plan.generation_id,),
            ).fetchone()
            if candidate is None or candidate["plan_digest"] != plan.plan_digest:
                raise CandidateValidationError("candidate does not match the materialization plan")
            if candidate["state"] not in {"WRITING", "VALIDATED", "ACTIVE", "RETIRED"}:
                raise CandidateValidationError("candidate state is not reconcilable")
            rows = self.candidate_records(plan.generation_id, page_size=7)
            observed_keys = [
                (row["customer_id"], row["feature_name"], row["record_digest"])
                for row in rows
            ]
            expected_keys = [
                (record.customer_id, record.feature_name, record.record_digest)
                for record in sorted(
                    plan.records, key=lambda item: (item.customer_id, item.feature_name)
                )
            ]
            if observed_keys != expected_keys:
                raise CandidateValidationError("candidate key or digest set is incomplete")
            for row in rows:
                stored: dict[str, Any] = json.loads(row["record_json"])
                recorded = stored.pop("record_digest", None)
                if recorded != row["record_digest"] or recorded != digest(stored):
                    raise CandidateValidationError("candidate record content is corrupt")
            actual_digest = digest([row["record_digest"] for row in rows])
            if (
                len(rows) != plan.expected_count
                or actual_digest != plan.records_digest
                or candidate["expected_count"] != plan.expected_count
                or candidate["expected_digest"] != plan.records_digest
            ):
                raise CandidateValidationError("candidate count or aggregate digest mismatch")
            receipt: dict[str, Any] = {
                "actual_count": len(rows),
                "actual_digest": actual_digest,
                "contract": VALIDATION_RECEIPT_CONTRACT,
                "definition_set_digest": plan.definition_set_digest,
                "feature_set": plan.feature_set,
                "generation_id": plan.generation_id,
                "offline_rows_digest": plan.offline_rows_digest,
                "operation_id": plan.operation_id,
                "plan_digest": plan.plan_digest,
                "source_digest": plan.source_digest,
            }
            receipt["receipt_digest"] = digest(receipt)
            rendered = canonical_json(receipt)
            if candidate["state"] != "WRITING":
                if (
                    candidate["receipt_json"] != rendered
                    or candidate["receipt_digest"] != receipt["receipt_digest"]
                ):
                    raise CandidateValidationError(
                        "validated candidate receipt no longer matches its records"
                    )
                return receipt
            self.connection.execute(
                """UPDATE candidates SET state='VALIDATED', receipt_json=?,
                   receipt_digest=? WHERE generation_id=?""",
                (rendered, receipt["receipt_digest"], plan.generation_id),
            )
            self.connection.execute(
                """UPDATE operations SET state='COMPLETED', result_json=?
                   WHERE operation_id=?""",
                (rendered, plan.operation_id),
            )
            return receipt

    def materialize(
        self, plan: MaterializationPlan, *, interrupt_after: int | None = None
    ) -> dict[str, Any]:
        replay = self.begin_materialization(plan)
        if replay is not None:
            return replay
        for confirmed, record in enumerate(self.missing_records(plan), start=1):
            self.write_record(plan, record)
            if interrupt_after is not None and confirmed == interrupt_after:
                raise MaterializationInterrupted(
                    "materialization interrupted after a durable write"
                )
        return self.reconcile(plan)

    def candidate_status(self, generation_id: str) -> str:
        row = self.connection.execute(
            "SELECT state FROM candidates WHERE generation_id=?", (generation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(generation_id)
        return str(row["state"])

    def pointer(self, feature_set: str) -> tuple[str | None, int]:
        row = self.connection.execute(
            "SELECT generation_id, pointer_version FROM pointers WHERE feature_set=?",
            (feature_set,),
        ).fetchone()
        if row is None:
            return None, 0
        return cast(str | None, row["generation_id"]), int(row["pointer_version"])

    def activate(
        self,
        generation_id: str,
        *,
        validation_receipt_digest: str,
        expected_generation: str | None,
        expected_version: int,
        operation_id: str,
        lose_acknowledgement: bool = False,
        actor: str = "stage4-local-proof",
        reason: str = "validated-candidate-promotion",
    ) -> dict[str, Any]:
        if not actor or not reason or len(actor) > 128 or len(reason) > 256:
            raise OnlineContractError("activation actor or reason is invalid")
        candidate_preflight = self.connection.execute(
            "SELECT feature_set FROM candidates WHERE generation_id=?",
            (generation_id,),
        ).fetchone()
        if candidate_preflight is None:
            raise ActivationConflict("candidate does not exist")
        feature_set = str(candidate_preflight["feature_set"])
        request = {
            "expected_generation": expected_generation,
            "expected_version": expected_version,
            "feature_set": feature_set,
            "generation_id": generation_id,
            "actor": actor,
            "reason": reason,
            "validation_receipt_digest": validation_receipt_digest,
        }
        request_digest = digest(request)
        with self._transaction():
            existing = self._operation(operation_id, "ACTIVATE", request_digest)
            if existing is not None and existing["state"] == "COMPLETED":
                return cast(dict[str, Any], json.loads(existing["result_json"]))
            candidate = self.connection.execute(
                "SELECT * FROM candidates WHERE generation_id=?", (generation_id,)
            ).fetchone()
            if (
                candidate is None
                or candidate["state"] != "VALIDATED"
                or candidate["receipt_digest"] != validation_receipt_digest
            ):
                raise ActivationConflict("candidate lacks the exact validation receipt")
            receipt: dict[str, Any] = json.loads(candidate["receipt_json"])
            rows = self.connection.execute(
                """SELECT record_json, record_digest FROM records
                   WHERE generation_id=? ORDER BY customer_id, feature_name""",
                (generation_id,),
            ).fetchall()
            for row in rows:
                body: dict[str, Any] = json.loads(row["record_json"])
                recorded = body.pop("record_digest", None)
                if recorded != row["record_digest"] or recorded != digest(body):
                    raise ActivationConflict("candidate changed after validation")
            if (
                len(rows) != receipt["actual_count"]
                or digest([row["record_digest"] for row in rows])
                != receipt["actual_digest"]
            ):
                raise ActivationConflict("candidate no longer matches validation")
            current_generation, current_version = self.pointer(feature_set)
            if (current_generation, current_version) != (
                expected_generation,
                expected_version,
            ):
                raise ActivationConflict("active pointer no longer matches expectation")
            changed = self.connection.execute(
                """UPDATE pointers SET generation_id=?, pointer_version=pointer_version+1
                   WHERE feature_set=? AND generation_id IS ? AND pointer_version=?""",
                (
                    generation_id,
                    feature_set,
                    expected_generation,
                    expected_version,
                ),
            )
            if changed.rowcount != 1:
                raise ActivationConflict("active pointer compare-and-swap failed")
            if current_generation is not None and current_generation != generation_id:
                self.connection.execute(
                    "UPDATE candidates SET state='RETIRED' WHERE generation_id=?",
                    (current_generation,),
                )
            self.connection.execute(
                "UPDATE candidates SET state='ACTIVE' WHERE generation_id=?",
                (generation_id,),
            )
            result: dict[str, Any] = {
                "contract": ACTIVATION_RECEIPT_CONTRACT,
                "actor": actor,
                "feature_set": feature_set,
                "from_generation": current_generation,
                "generation_id": generation_id,
                "operation_id": operation_id,
                "pointer_version": expected_version + 1,
                "reason": reason,
                "validation_receipt_digest": validation_receipt_digest,
            }
            result["receipt_digest"] = digest(result)
            rendered = canonical_json(result)
            if existing is None:
                self.connection.execute(
                    "INSERT INTO operations VALUES (?, 'ACTIVATE', ?, 'COMPLETED', ?)",
                    (operation_id, request_digest, rendered),
                )
            self.connection.execute(
                "INSERT INTO pointer_history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    feature_set,
                    expected_version + 1,
                    current_generation,
                    generation_id,
                    operation_id,
                    actor,
                    reason,
                    result["receipt_digest"],
                ),
            )
        if lose_acknowledgement:
            raise AcknowledgementLost(
                "activation committed but acknowledgement was deliberately lost"
            )
        return result

    def pin_reader(self, feature_set: str) -> ReaderToken:
        generation_id, version = self.pointer(feature_set)
        if generation_id is None:
            raise ActivationConflict("feature set has no active generation")
        return ReaderToken(feature_set, generation_id, version)

    def read(
        self,
        token: ReaderToken,
        customer_id: str,
        feature_name: str,
        *,
        definition_digest: str,
        request_time: int,
        maximum_freshness_age: int | None = None,
    ) -> ServingResult:
        try:
            candidate = self.connection.execute(
                "SELECT feature_set, state FROM candidates WHERE generation_id=?",
                (token.generation_id,),
            ).fetchone()
            if (
                candidate is None
                or candidate["feature_set"] != token.feature_set
                or candidate["state"] not in {"ACTIVE", "RETIRED"}
            ):
                return ServingResult(
                    "GENERATION_UNAVAILABLE",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                )
            row = self.connection.execute(
                """SELECT * FROM records WHERE generation_id=?
                   AND customer_id=? AND feature_name=?""",
                (token.generation_id, customer_id, feature_name),
            ).fetchone()
            if row is None:
                return ServingResult(
                    "ABSENT",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                )
            if row["definition_digest"] != definition_digest:
                return ServingResult(
                    "DEFINITION_MISMATCH",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                    definition_digest=str(row["definition_digest"]),
                    record_digest=str(row["record_digest"]),
                )
            stored: dict[str, Any] = json.loads(row["record_json"])
            recorded = stored.pop("record_digest", None)
            if recorded != row["record_digest"] or recorded != digest(stored):
                return ServingResult(
                    "CORRUPT_RECORD",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                    definition_digest=definition_digest,
                    record_digest=str(row["record_digest"]),
                )
            if request_time >= int(row["expires_at"]):
                return ServingResult(
                    "EXPIRED",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                    definition_digest=definition_digest,
                    record_digest=str(row["record_digest"]),
                )
            if (
                maximum_freshness_age is not None
                and request_time - int(stored["knowledge_cutoff"])
                > maximum_freshness_age
            ):
                return ServingResult(
                    "STALE",
                    token.feature_set,
                    token.generation_id,
                    customer_id,
                    feature_name,
                    definition_digest=definition_digest,
                    record_digest=str(row["record_digest"]),
                )
            raw_value = _decode_value(cast(dict[str, Any], stored["value"]))
            return ServingResult(
                "FOUND",
                token.feature_set,
                token.generation_id,
                customer_id,
                feature_name,
                definition_digest=definition_digest,
                record_digest=str(row["record_digest"]),
                value=raw_value,
            )
        except OnlineContractError:
            return ServingResult(
                "CORRUPT_RECORD",
                token.feature_set,
                token.generation_id,
                customer_id,
                feature_name,
                definition_digest=definition_digest,
            )
        except sqlite3.Error:
            return ServingResult(
                "STORAGE_FAILURE",
                token.feature_set,
                token.generation_id,
                customer_id,
                feature_name,
            )

    def history(self, feature_set: str) -> tuple[dict[str, Any], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                """SELECT * FROM pointer_history WHERE feature_set=?
                   ORDER BY pointer_version""",
                (feature_set,),
            )
        )

    def record_retry_attempt(
        self,
        operation_id: str,
        *,
        attempt: int,
        outcome: Literal["RETRYABLE", "CONFIRMED", "TERMINAL"],
        confirmed_count: int,
        next_retry_at: int | None,
        terminal_reason: str | None,
    ) -> str:
        if attempt < 1 or confirmed_count < 0:
            raise OnlineContractError("retry attempt and confirmed count must be non-negative")
        if outcome == "RETRYABLE" and next_retry_at is None:
            raise OnlineContractError("retryable attempt requires its next eligible time")
        if outcome != "RETRYABLE" and next_retry_at is not None:
            raise OnlineContractError("non-retryable attempt cannot schedule another retry")
        body = {
            "attempt": attempt,
            "confirmed_count": confirmed_count,
            "next_retry_at": next_retry_at,
            "operation_id": operation_id,
            "outcome": outcome,
            "terminal_reason": terminal_reason,
        }
        attempt_digest = digest(body)
        with self._transaction():
            operation = self.connection.execute(
                "SELECT operation_id FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if operation is None:
                raise OnlineStateError("retry attempt has no durable operation")
            existing = self.connection.execute(
                """SELECT attempt_digest FROM retry_attempts
                   WHERE operation_id=? AND attempt=?""",
                (operation_id, attempt),
            ).fetchone()
            if existing is not None:
                if existing["attempt_digest"] != attempt_digest:
                    raise OnlineConflict("retry attempt identity was reused with different content")
                return "replayed"
            self.connection.execute(
                """INSERT INTO retry_attempts
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    operation_id,
                    attempt,
                    outcome,
                    confirmed_count,
                    next_retry_at,
                    terminal_reason,
                    attempt_digest,
                ),
            )
        return "recorded"

    def retry_attempts(self, operation_id: str) -> tuple[dict[str, Any], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                """SELECT * FROM retry_attempts WHERE operation_id=?
                   ORDER BY attempt""",
                (operation_id,),
            )
        )


@dataclass(frozen=True)
class RetryPolicy:
    maximum_attempts: int
    base_delay_ms: int
    maximum_delay_ms: int
    seed: int

    def __post_init__(self) -> None:
        if (
            self.maximum_attempts < 1
            or self.base_delay_ms < 1
            or self.maximum_delay_ms < self.base_delay_ms
        ):
            raise OnlineContractError("invalid bounded retry policy")

    def delays(self) -> tuple[int, ...]:
        generator = random.Random(self.seed)
        return tuple(
            min(
                self.maximum_delay_ms,
                self.base_delay_ms * (2**attempt)
                + generator.randrange(self.base_delay_ms),
            )
            for attempt in range(self.maximum_attempts - 1)
        )


def capacity_report(plan: MaterializationPlan) -> dict[str, Any]:
    sizes = [
        len(canonical_json(dynamodb_record_item(record)).encode("utf-8"))
        for record in plan.records
    ]
    by_partition: dict[str, int] = {}
    for record in plan.records:
        by_partition[record.partition_key] = by_partition.get(record.partition_key, 0) + 1
    maximum = max(sizes, default=0)
    return {
        "contract": "online-capacity-report-v1",
        "item_count": len(sizes),
        "maximum_item_bytes": maximum,
        "maximum_item_headroom_bytes": MAX_DYNAMODB_ITEM_BYTES - maximum,
        "maximum_records_per_partition": max(by_partition.values(), default=0),
        "partition_count": len(by_partition),
        "put_request_count": len(sizes),
        "service_item_limit_bytes": MAX_DYNAMODB_ITEM_BYTES,
        "total_item_bytes": sum(sizes),
    }
