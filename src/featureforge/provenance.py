"""Canonical, immutable provenance for the local FeatureForge lifecycle."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from featureforge.canonical import digest
from featureforge.model import FeatureDefinition, Label, PaymentEvent
from featureforge.temporal import normalize_events

PAYMENT_EVENT_FIELDS = {
    "amount_cents",
    "customer_id",
    "event_id",
    "event_time",
    "knowledge_time",
    "merchant_risk",
    "operation",
    "revision_id",
    "status",
}
LABEL_FIELDS = {
    "customer_id",
    "label_id",
    "label_time",
    "prediction_knowledge_time",
    "value",
}


class ContractError(ValueError):
    """Input does not conform to the declared versioned contract."""


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    kind: str
    manifest: dict[str, Any]
    rows: tuple[dict[str, Any], ...]
    artifact_digest: str


def _require_exact_fields(row: dict[str, Any], expected: set[str], contract: str) -> None:
    if set(row) != expected:
        missing = sorted(expected - set(row))
        unknown = sorted(set(row) - expected)
        raise ContractError(f"{contract}: missing={missing}, unknown={unknown}")


def _require_string(value: Any, field: str, contract: str) -> None:
    if not isinstance(value, str) or not value:
        raise ContractError(f"{contract}: {field} must be a non-empty string")


def _require_integer(value: Any, field: str, contract: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{contract}: {field} must be an integer")


def _require_number_or_null(value: Any, field: str, contract: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
        raise ContractError(f"{contract}: {field} must be numeric or null")


def decode_payment_events(rows: list[dict[str, Any]]) -> tuple[PaymentEvent, ...]:
    """Strictly decode and canonically normalize a v2 payment-event snapshot."""

    decoded: list[PaymentEvent] = []
    for position, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ContractError(f"payment-event-v2 row {position} is not an object")
        _require_exact_fields(row, PAYMENT_EVENT_FIELDS, f"payment-event-v2 row {position}")
        contract = f"payment-event-v2 row {position}"
        _require_string(row["event_id"], "event_id", contract)
        _require_string(row["customer_id"], "customer_id", contract)
        _require_string(row["revision_id"], "revision_id", contract)
        _require_string(row["operation"], "operation", contract)
        _require_integer(row["event_time"], "event_time", contract)
        _require_integer(row["knowledge_time"], "knowledge_time", contract)
        if row["amount_cents"] is not None:
            _require_integer(row["amount_cents"], "amount_cents", contract)
        _require_number_or_null(row["merchant_risk"], "merchant_risk", contract)
        if row["status"] is not None and not isinstance(row["status"], str):
            raise ContractError(f"{contract}: status must be a string or null")
        try:
            decoded.append(PaymentEvent(**row))
        except (TypeError, ValueError) as error:
            raise ContractError(f"payment-event-v2 row {position}: {error}") from error
    return normalize_events(decoded)


def decode_labels(rows: list[dict[str, Any]]) -> tuple[Label, ...]:
    """Strictly decode and deterministically order training requests."""

    labels: list[Label] = []
    seen: set[str] = set()
    for position, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ContractError(f"training-request-v1 row {position} is not an object")
        _require_exact_fields(row, LABEL_FIELDS, f"training-request-v1 row {position}")
        contract = f"training-request-v1 row {position}"
        _require_string(row["label_id"], "label_id", contract)
        _require_string(row["customer_id"], "customer_id", contract)
        _require_integer(row["label_time"], "label_time", contract)
        _require_integer(row["prediction_knowledge_time"], "prediction_knowledge_time", contract)
        _require_integer(row["value"], "value", contract)
        try:
            label = Label(**row)
        except (TypeError, ValueError) as error:
            raise ContractError(f"training-request-v1 row {position}: {error}") from error
        if label.label_id in seen:
            raise ContractError(f"duplicate label identity: {label.label_id}")
        seen.add(label.label_id)
        labels.append(label)
    return tuple(sorted(labels, key=lambda item: item.label_id))


def _artifact(
    artifact_id: str,
    kind: str,
    contract: str,
    rows: tuple[dict[str, Any], ...],
    metadata: dict[str, Any],
) -> Artifact:
    if not artifact_id:
        raise ValueError("artifact identity must be non-empty")
    rows_digest = digest(rows)
    manifest: dict[str, Any] = {
        "artifact_id": artifact_id,
        "contract": contract,
        "kind": kind,
        "metadata": metadata,
        "row_count": len(rows),
        "rows_digest": rows_digest,
    }
    manifest["manifest_digest"] = digest(manifest)
    artifact_digest = digest(
        {
            "contract": "featureforge-artifact-v1",
            "manifest": manifest,
            "rows": rows,
        }
    )
    return Artifact(artifact_id, kind, manifest, rows, artifact_digest)


def rows_artifact(
    artifact_id: str,
    kind: str,
    contract: str,
    rows: tuple[dict[str, Any], ...],
    metadata: dict[str, Any],
) -> Artifact:
    """Create a domain-separated immutable artifact from canonical row content."""

    if not kind or not contract:
        raise ValueError("artifact kind and contract must be non-empty")
    return _artifact(artifact_id, kind, contract, rows, metadata)


def source_snapshot(snapshot_id: str, raw_rows: list[dict[str, Any]]) -> Artifact:
    events = decode_payment_events(raw_rows)
    rows = tuple(event.as_dict() for event in events)
    metadata = {
        "distinct_event_count": len({event.event_id for event in events}),
        "event_time_max": max((event.event_time for event in events), default=None),
        "event_time_min": min((event.event_time for event in events), default=None),
        "knowledge_time_max": max((event.knowledge_time for event in events), default=None),
        "knowledge_time_min": min((event.knowledge_time for event in events), default=None),
    }
    return _artifact(snapshot_id, "source", "payment-event-v2", rows, metadata)


def label_snapshot(snapshot_id: str, raw_rows: list[dict[str, Any]]) -> Artifact:
    labels = decode_labels(raw_rows)
    rows = tuple(asdict(label) for label in labels)
    prediction_cutoffs = [label.prediction_knowledge_time for label in labels]
    if any(value is None for value in prediction_cutoffs):
        raise ContractError("prediction knowledge cutoff was not normalized")
    normalized_cutoffs = [value for value in prediction_cutoffs if value is not None]
    metadata = {
        "event_cutoff_max": max((label.label_time for label in labels), default=None),
        "event_cutoff_min": min((label.label_time for label in labels), default=None),
        "prediction_knowledge_max": max(normalized_cutoffs, default=None),
        "prediction_knowledge_min": min(normalized_cutoffs, default=None),
    }
    return _artifact(snapshot_id, "labels", "training-request-v1", rows, metadata)


def definition_set(definitions: tuple[FeatureDefinition, ...]) -> dict[str, Any]:
    ordered = tuple(sorted(definitions, key=lambda item: item.definition_id))
    identities = [item.definition_id for item in ordered]
    if len(identities) != len(set(identities)):
        raise ContractError("definition set contains duplicate name/version identities")
    rows = [asdict(item) | {"definition_digest": item.definition_digest} for item in ordered]
    result: dict[str, Any] = {
        "contract": "feature-definition-set-v1",
        "definitions": rows,
    }
    result["definition_set_digest"] = digest(result)
    return result


def bind_dataset_manifest(
    production_manifest: dict[str, Any],
    *,
    source: Artifact,
    labels: Artifact,
    definition_set_digest: str,
    code_tree: str,
) -> dict[str, Any]:
    """Bind a deterministic training result to immutable input and code identities."""

    result: dict[str, Any] = {
        "code_tree": code_tree,
        "contract": "training-dataset-manifest-v2",
        "definition_set_digest": definition_set_digest,
        "label_snapshot_digest": labels.artifact_digest,
        "production_manifest": production_manifest,
        "source_snapshot_digest": source.artifact_digest,
    }
    result["manifest_digest"] = digest(result)
    return result


def rejection_report(
    snapshot_id: str, raw_rows: list[dict[str, Any]], error: Exception
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "contract": "source-rejection-report-v1",
        "error_type": type(error).__name__,
        "message": str(error),
        "raw_digest": digest(raw_rows),
        "snapshot_id": snapshot_id,
    }
    result["report_digest"] = digest(result)
    return result
