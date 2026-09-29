"""Stage 6 Lambda control worker.

The handler is intentionally a thin dispatcher over immutable authorities.  AWS
clients are injected for tests and created lazily only when Lambda invokes the
module; importing this file cannot contact AWS.
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Mapping, Sequence
from typing import Any

from featureforge.aws_runtime import (
    DynamoTaskLedger,
    S3Client,
    TaskLedgerBoundary,
    read_exact_object,
)
from featureforge.canonical import digest
from featureforge.managed import (
    AdmissionDenied,
    ManagedContractError,
    ManagedRunManifest,
    S3ObjectAuthority,
    completion_receipt,
    task_request,
)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManagedContractError(f"{name} must be an object")
    return value


def _authority(value: Mapping[str, Any]) -> S3ObjectAuthority:
    return S3ObjectAuthority(
        bucket=str(value["bucket"]),
        key=str(value["key"]),
        version_id=str(value["version_id"]),
        sha256=str(value["sha256"]),
    )


class ControlWorker:
    def __init__(self, *, s3: S3Client, ledger: TaskLedgerBoundary) -> None:
        self._s3 = s3
        self._ledger = ledger

    def dispatch(self, event: Mapping[str, Any]) -> dict[str, Any]:
        action = str(event.get("action", ""))
        manifest = ManagedRunManifest.from_dict(_mapping(event.get("manifest"), "manifest"))
        authority = _authority(_mapping(event.get("manifest_authority"), "manifest_authority"))
        exact = read_exact_object(self._s3, authority)
        manifest_bytes = json.dumps(
            manifest.as_dict(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if exact.body != manifest_bytes:
            raise ManagedContractError("manifest object bytes differ from supplied authority")
        payload = dict(_mapping(event.get("payload", {}), "payload"))
        started = task_request(manifest.run_id, action, manifest.manifest_digest, payload)
        replay = self._ledger.begin(started)
        if replay.get("state") == "COMPLETED":
            if not isinstance(replay.get("result"), Mapping):
                raise ManagedContractError("completed task has no replayable result")
            return replay
        result = self._execute(action, manifest, payload)
        receipt = self._ledger.complete(manifest.run_id, action, result)
        return receipt | {"result": result}

    def _execute(
        self, action: str, manifest: ManagedRunManifest, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        if action == "VALIDATE":
            return {
                "decision": "ELIGIBLE",
                "manifest_digest": manifest.manifest_digest,
                "run_id": manifest.run_id,
            }
        if action == "RECORD_GLUE":
            if payload.get("state") not in {"SUCCEEDED", "SUCCEEDED_WITH_WARNINGS"}:
                raise AdmissionDenied("Glue completion is not successful")
            job_run_id = str(payload.get("glue_job_run_id", ""))
            if not job_run_id:
                raise ManagedContractError("Glue completion lacks job-run identity")
            return {"glue_job_run_id": job_run_id, "state": str(payload["state"])}
        if action == "MATERIALIZE_ONLINE":
            payload_authority = _authority(
                _mapping(payload.get("online_payload_authority"), "online_payload_authority")
            )
            exact = read_exact_object(self._s3, payload_authority)
            content = json.loads(exact.body)
            if not isinstance(content, list) or len(content) > manifest.max_output_rows:
                raise ManagedContractError("online payload is not a bounded record list")
            records = sorted(
                (_mapping(row, "online record") for row in content),
                key=lambda row: (str(row.get("customer_id")), str(row.get("feature_name"))),
            )
            identities = [
                (str(row.get("customer_id")), str(row.get("feature_name"))) for row in records
            ]
            if len(identities) != len(set(identities)):
                raise ManagedContractError("online payload contains duplicate feature keys")
            return {
                "candidate_count": len(records),
                "candidate_digest": digest(records),
                "payload_authority": payload_authority.as_dict(),
            }
        if action == "PARITY":
            expected = str(payload.get("expected_digest", ""))
            observed = str(payload.get("observed_digest", ""))
            if len(expected) != 64 or expected != observed:
                return {"decision": "QUARANTINED", "reason": "independent parity mismatch"}
            return {"decision": "ELIGIBLE", "parity_digest": digest(dict(payload))}
        if action == "ACTIVATE":
            if payload.get("parity_decision") != "ELIGIBLE":
                raise AdmissionDenied("activation requires an eligible independent parity result")
            required = ("feature_set", "generation_id", "expected_version")
            if any(name not in payload for name in required):
                raise ManagedContractError("activation payload is incomplete")
            return {
                "activation_digest": digest(dict(payload)),
                "feature_set": payload["feature_set"],
                "generation_id": payload["generation_id"],
                "pointer_version": int(payload["expected_version"]) + 1,
            }
        if action == "COMPLETE":
            admission_value = _mapping(payload.get("admission"), "admission")
            task_receipts = payload.get("task_receipts")
            outputs = payload.get("output_objects")
            if not isinstance(task_receipts, Sequence) or isinstance(task_receipts, (str, bytes)):
                raise ManagedContractError("completion task receipts are invalid")
            if not isinstance(outputs, Sequence) or isinstance(outputs, (str, bytes)):
                raise ManagedContractError("completion outputs are invalid")
            # Completion uses only the immutable admission digest supplied by the
            # earlier admission authority; reconstruct a minimal structural proxy.
            admission_digest = str(admission_value.get("admission_digest", ""))
            if len(admission_digest) != 64:
                raise ManagedContractError("completion admission digest is invalid")
            output_authorities = tuple(_authority(_mapping(row, "output")) for row in outputs)
            body: dict[str, Any] = {
                "admission_digest": admission_digest,
                "contract": "stage6-completion-receipt-v1",
                "manifest_digest": manifest.manifest_digest,
                "output_objects": [item.as_dict() for item in output_authorities],
                "run_id": manifest.run_id,
                "task_receipts_digest": digest(
                    [dict(_mapping(row, "task receipt")) for row in task_receipts]
                ),
            }
            return body | {"receipt_digest": digest(body)}
        raise ManagedContractError("unsupported control-worker action")


def _lambda_worker() -> ControlWorker:
    boto3: Any = importlib.import_module("boto3")
    table_name = os.environ.get("CONTROL_TABLE", "")
    if not table_name:
        raise RuntimeError("Stage 6 control table is not configured")
    s3: S3Client = boto3.client("s3")
    ledger = DynamoTaskLedger(boto3.client("dynamodb"), table_name)
    return ControlWorker(s3=s3, ledger=ledger)


def lambda_handler(event: Mapping[str, Any], _context: Any) -> dict[str, Any]:
    if os.environ.get("FEATUREFORGE_STAGE6_ENABLED") != "true":
        raise RuntimeError("Stage 6 control worker is disabled by default")
    return _lambda_worker().dispatch(event)


__all__ = ["ControlWorker", "lambda_handler", "completion_receipt"]
