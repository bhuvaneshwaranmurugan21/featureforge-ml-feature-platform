"""Stage 6 Lambda control worker.

The handler is intentionally a thin dispatcher over immutable authorities.  AWS
clients are injected for tests and created lazily only when Lambda invokes the
module; importing this file cannot contact AWS.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from featureforge.aws_runtime import (
    DynamoOnlineRuntime,
    DynamoTaskLedger,
    S3Client,
    TaskLedgerBoundary,
    read_exact_object,
)
from featureforge.canonical import digest
from featureforge.expected import (
    compare_online_records,
    project_feature_rows,
    project_online_records,
)
from featureforge.glue_launch import DurableGlueLauncher
from featureforge.managed import (
    AdmissionDenied,
    ManagedContractError,
    ManagedRunManifest,
    S3ObjectAuthority,
    completion_receipt,
    task_request,
)
from featureforge.managed_admission import AWSManagedAdmission, object_authority
from featureforge.model import FeatureDefinition, FeatureValue
from featureforge.online import MaterializationPlan, build_materialization_plan


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManagedContractError(f"{name} must be an object")
    return value


def _authority(value: Mapping[str, Any]) -> S3ObjectAuthority:
    return object_authority(value)


class ManagedAdmissionBoundary(Protocol):
    def verify(self, manifest: ManagedRunManifest) -> Mapping[str, Any]: ...

    def verify_current(
        self, manifest: ManagedRunManifest, prior_receipt: Mapping[str, Any]
    ) -> None: ...


class ControlWorker:
    def __init__(
        self,
        *,
        s3: S3Client,
        ledger: TaskLedgerBoundary,
        online: DynamoOnlineRuntime | None = None,
        admission: ManagedAdmissionBoundary | None = None,
        glue_launcher: DurableGlueLauncher | None = None,
    ) -> None:
        self._s3 = s3
        self._ledger = ledger
        self._online = online
        self._admission = admission
        self._glue_launcher = glue_launcher

    def _prior(self, manifest: ManagedRunManifest, task: str) -> Mapping[str, Any]:
        receipt = self._ledger.get(manifest.run_id, task)
        if (
            receipt is None
            or receipt.get("state") != "COMPLETED"
            or receipt.get("manifest_digest") != manifest.manifest_digest
        ):
            raise AdmissionDenied(f"{task} has no durable matching completed authority")
        return _mapping(receipt.get("result"), "prior task result")

    def _plan(
        self, manifest: ManagedRunManifest, payload: Mapping[str, Any]
    ) -> tuple[MaterializationPlan, tuple[dict[str, Any], ...]]:
        if self._online is None:
            raise AdmissionDenied("managed online adapter is not configured")
        admission = self._prior(manifest, "VALIDATE")
        glue = self._prior(manifest, "RECORD_GLUE")
        source = _mapping(
            json.loads(
                read_exact_object(self._s3, manifest.inputs[0], maximum_bytes=32 * 1024 * 1024).body
            ),
            "source",
        )
        authority = _authority(
            _mapping(payload.get("online_payload_authority"), "online_payload_authority")
        )
        if authority.as_dict() != glue.get("online_payload_authority"):
            raise AdmissionDenied("online rows do not match durable Glue output authority")
        if (
            authority.bucket != manifest.output_bucket
            or authority.key != manifest.output_prefix + "rows.json"
        ):
            raise ManagedContractError("online rows are outside the immutable run prefix")
        rows = json.loads(read_exact_object(self._s3, authority).body)
        if not isinstance(rows, list) or not rows or len(rows) > manifest.max_output_rows:
            raise ManagedContractError("online rows must be a nonempty bounded list")
        raw_definitions = source.get("definitions")
        raw_events = source.get("events")
        customers = source.get("customer_ids")
        if (
            not isinstance(raw_definitions, list)
            or not isinstance(raw_events, list)
            or not isinstance(customers, list)
            or len(raw_events) > manifest.max_input_rows
        ):
            raise ManagedContractError("immutable source rows or definitions are invalid")
        definitions = tuple(
            FeatureDefinition(**dict(_mapping(row, "definition"))) for row in raw_definitions
        )
        definition_rows = tuple(
            dict(_mapping(row, "definition")) | {"definition_digest": definition.definition_digest}
            for row, definition in zip(raw_definitions, definitions, strict=True)
        )
        values = tuple(FeatureValue(**dict(_mapping(row, "offline feature row"))) for row in rows)
        generation_id = str(source["generation_id"])
        if any(value.generation_id != generation_id for value in values):
            raise ManagedContractError("offline generation differs from immutable source")
        feature_set = str(payload.get("feature_set", ""))
        materialized_at = payload.get("materialized_at")
        if isinstance(materialized_at, bool) or not isinstance(materialized_at, int):
            raise ManagedContractError("materialization time must be an integer")
        plan = build_materialization_plan(
            feature_set,
            manifest.run_id,
            definitions,
            values,
            source_digest=manifest.inputs[0].sha256,
            materialized_at=materialized_at,
        )
        expected_rows = admission.get("frozen_expected")
        if (
            not isinstance(expected_rows, list)
            or digest(expected_rows) != admission.get("frozen_expected_digest")
            or admission.get("frozen_source_sha256") != manifest.inputs[0].sha256
            or admission.get("frozen_definitions") != list(definition_rows)
        ):
            raise AdmissionDenied("pre-run expected authority is missing or corrupt")
        ordered_rows = sorted(rows, key=lambda row: (row["customer_id"], row["feature_name"]))
        if ordered_rows != expected_rows:
            raise AdmissionDenied("offline output differs from pre-run frozen independent state")
        expected = project_online_records(
            definition_rows,
            expected_rows,
            feature_set=feature_set,
            operation_id=manifest.run_id,
            source_digest=manifest.inputs[0].sha256,
            materialized_at=materialized_at,
        )
        return plan, expected

    def dispatch(
        self, event: Mapping[str, Any], *, invocation_id: str | None = None
    ) -> dict[str, Any]:
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
        admitted_result = self._execute(action, manifest, payload) if action == "VALIDATE" else None
        if action != "VALIDATE":
            admitted = self._prior(manifest, "VALIDATE")
            if self._admission is None:
                raise AdmissionDenied("current managed admission boundary is not configured")
            self._admission.verify_current(manifest, admitted)
        replay = self._ledger.begin(started)
        if replay.get("state") == "COMPLETED":
            if not isinstance(replay.get("result"), Mapping):
                raise ManagedContractError("completed task has no replayable result")
            return replay
        result = (
            admitted_result
            if admitted_result is not None
            else self._execute(action, manifest, payload, invocation_id=invocation_id)
        )
        receipt = self._ledger.complete(manifest.run_id, action, result)
        return receipt | {"result": result}

    def _execute(
        self,
        action: str,
        manifest: ManagedRunManifest,
        payload: Mapping[str, Any],
        *,
        invocation_id: str | None = None,
    ) -> dict[str, Any]:
        if action == "VALIDATE":
            if self._admission is None:
                raise AdmissionDenied("verified managed admission boundary is not configured")
            verified = dict(self._admission.verify(manifest))
            if (
                verified.get("decision") != "ELIGIBLE"
                or verified.get("manifest_digest") != manifest.manifest_digest
                or verified.get("run_id") != manifest.run_id
            ):
                raise AdmissionDenied("verified admission does not bind the run")
            execution_id = payload.get("execution_id")
            if (
                not isinstance(execution_id, str)
                or not execution_id
                or len(execution_id) > 256
                or execution_id.strip() != execution_id
            ):
                raise ManagedContractError("VALIDATE requires a bounded execution identity")
            source = _mapping(
                json.loads(
                    read_exact_object(
                        self._s3, manifest.inputs[0], maximum_bytes=32 * 1024 * 1024
                    ).body
                ),
                "immutable source",
            )
            definitions = source.get("definitions")
            events = source.get("events")
            customers = source.get("customer_ids")
            if (
                not isinstance(definitions, list)
                or not definitions
                or not isinstance(events, list)
                or len(events) > manifest.max_input_rows
                or not isinstance(customers, list)
                or not customers
                or len(definitions) * len(customers) > manifest.max_output_rows
            ):
                raise ManagedContractError("pre-run primitive projection exceeds bounded contracts")
            definitions_with_digest = [
                dict(_mapping(row, "definition"))
                | {
                    "definition_digest": FeatureDefinition(
                        **dict(_mapping(row, "definition"))
                    ).definition_digest
                }
                for row in definitions
            ]
            frozen = list(
                project_feature_rows(
                    definitions_with_digest,
                    events,
                    customers,
                    str(source["generation_id"]),
                    event_cutoff=int(source["event_cutoff"]),
                    knowledge_cutoff=int(source["knowledge_cutoff"]),
                )
            )
            expected_authority = {
                "execution_id": execution_id,
                "frozen_expected": frozen,
                "frozen_expected_digest": digest(frozen),
                "frozen_definitions": definitions_with_digest,
                "frozen_source_sha256": manifest.inputs[0].sha256,
            }
            for key, value in expected_authority.items():
                if key in verified and verified[key] != value:
                    raise AdmissionDenied(
                        "admission contains conflicting frozen expected authority"
                    )
            return verified | expected_authority
        if action == "START_GLUE":
            admitted = self._prior(manifest, "VALIDATE")
            execution_id = payload.get("execution_id")
            if not isinstance(execution_id, str) or execution_id != admitted.get("execution_id"):
                raise AdmissionDenied("Glue launch differs from durable execution owner")
            if self._glue_launcher is None or invocation_id is None:
                raise AdmissionDenied("durable Glue launcher and invocation identity are required")
            return self._glue_launcher.start(
                manifest, execution_id=execution_id, invocation_id=invocation_id
            )
        if action == "RECORD_GLUE":
            launched = self._prior(manifest, "START_GLUE")
            if payload.get("glue_job_run_id") != launched.get("JobRunId") or payload.get(
                "job_name"
            ) != launched.get("JobName"):
                raise AdmissionDenied("Glue completion differs from durable launch identity")
            if payload.get("state") != "SUCCEEDED":
                raise AdmissionDenied("Glue completion is not successful")
            job_run_id = str(payload.get("glue_job_run_id", ""))
            if not job_run_id:
                raise ManagedContractError("Glue completion lacks job-run identity")
            response = self._s3.get_object(
                Bucket=manifest.output_bucket, Key=manifest.output_prefix + "output-authority.json"
            )
            body_value = response.get("Body")
            if isinstance(body_value, bytes):
                authority_bytes = body_value
            else:
                reader = getattr(body_value, "read", None)
                if not callable(reader):
                    raise ManagedContractError("Glue output authority body is unreadable")
                authority_bytes = reader()
            version = response.get("VersionId")
            if (
                not isinstance(authority_bytes, bytes)
                or not isinstance(version, str)
                or not version
            ):
                raise ManagedContractError("Glue output authority lacks immutable version")
            authority = S3ObjectAuthority(
                manifest.output_bucket,
                manifest.output_prefix + "output-authority.json",
                version,
                hashlib.sha256(authority_bytes).hexdigest(),
            )
            content = dict(
                _mapping(
                    json.loads(read_exact_object(self._s3, authority).body), "Glue output authority"
                )
            )
            observed_digest = content.pop("authority_digest", None)
            if (
                content.get("contract") != "stage6-glue-output-authority-v2"
                or observed_digest != digest(content)
                or content.get("input_authority") != manifest.inputs[0].as_dict()
                or content.get("launch_authority_digest") != launched.get("launch_authority_digest")
            ):
                raise ManagedContractError(
                    "Glue output authority does not bind the source and launch"
                )
            rows_authority = _authority(
                _mapping(content.get("rows_authority"), "Glue rows authority")
            )
            generation_authority = _authority(
                _mapping(content.get("manifest_authority"), "Glue generation authority")
            )
            for bound, suffix in (
                (rows_authority, "rows.json"),
                (generation_authority, "manifest.json"),
            ):
                if (
                    bound.bucket != manifest.output_bucket
                    or bound.key != manifest.output_prefix + suffix
                ):
                    raise ManagedContractError("Glue output escaped the run prefix")
                read_exact_object(self._s3, bound)
            return {
                "glue_job_run_id": job_run_id,
                "state": str(payload["state"]),
                "online_payload_authority": rows_authority.as_dict(),
                "generation_manifest_authority": generation_authority.as_dict(),
                "output_authority": authority.as_dict(),
                "output_objects": [
                    rows_authority.as_dict(),
                    generation_authority.as_dict(),
                    authority.as_dict(),
                ],
            }
        if action == "MATERIALIZE_ONLINE":
            plan, _expected = self._plan(manifest, payload)
            assert self._online is not None
            receipt = self._online.materialize(plan)
            return {
                "candidate_count": plan.expected_count,
                "candidate_digest": plan.records_digest,
                "plan_digest": plan.plan_digest,
                "validation_receipt": receipt,
                "materialization_context": dict(payload),
            }
        if action == "PARITY":
            prior = self._prior(manifest, "MATERIALIZE_ONLINE")
            if prior.get("materialization_context") != dict(payload):
                raise AdmissionDenied("parity materialization authority changed")
            plan, expected = self._plan(manifest, payload)
            assert self._online is not None
            actual = self._online.records(plan)
            request_time = payload.get("request_time")
            age = payload.get("maximum_freshness_age")
            if (
                isinstance(request_time, bool)
                or not isinstance(request_time, int)
                or isinstance(age, bool)
                or not isinstance(age, int)
                or age < 0
                or request_time < int(payload["materialized_at"])
            ):
                raise ManagedContractError("parity freshness clocks are invalid")
            report = compare_online_records(
                expected, actual, request_time=request_time, maximum_freshness_age=age
            )
            result = {
                "decision": "ELIGIBLE" if report["passed"] else "QUARANTINED",
                "parity_report": report,
                "plan_digest": plan.plan_digest,
                "validation_receipt": prior["validation_receipt"],
            }
            return result | {"parity_digest": digest(result)}
        if action == "ACTIVATE":
            parity = self._prior(manifest, "PARITY")
            if parity.get("decision") != "ELIGIBLE" or parity.get("parity_digest") != payload.get(
                "parity_receipt_digest"
            ):
                raise AdmissionDenied("activation requires exact durable eligible parity")
            materialization = self._prior(manifest, "MATERIALIZE_ONLINE")
            plan, _expected = self._plan(
                manifest,
                _mapping(materialization.get("materialization_context"), "materialization context"),
            )
            assert self._online is not None
            self._online.records(plan)
            version = payload.get("expected_version")
            generation = payload.get("expected_generation")
            if (
                isinstance(version, bool)
                or not isinstance(version, int)
                or version < 0
                or (generation is not None and not isinstance(generation, str))
                or (generation is None and version != 0)
            ):
                raise ManagedContractError("activation pointer authority is invalid")
            validation = _mapping(materialization.get("validation_receipt"), "validation receipt")
            return self._online.activate(
                plan=plan,
                validation_receipt_digest=str(validation["receipt_digest"]),
                expected_generation=generation,
                expected_version=version,
                operation_id=manifest.run_id + "-activate",
            )
        if action == "COMPLETE":
            admission_value = self._prior(manifest, "VALIDATE")
            task_receipts = [
                self._ledger.get(manifest.run_id, name)
                for name in (
                    "VALIDATE",
                    "START_GLUE",
                    "RECORD_GLUE",
                    "MATERIALIZE_ONLINE",
                    "PARITY",
                    "ACTIVATE",
                )
            ]
            for name in ("START_GLUE", "RECORD_GLUE", "MATERIALIZE_ONLINE", "PARITY", "ACTIVATE"):
                self._prior(manifest, name)
            glue = self._prior(manifest, "RECORD_GLUE")
            outputs = payload.get("output_objects")
            if outputs != glue.get("output_objects"):
                raise AdmissionDenied(
                    "completion output authorities differ from durable Glue receipt"
                )
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
    online_table = os.environ.get("ONLINE_TABLE", "")
    if not table_name or not online_table:
        raise RuntimeError("Stage 6 control and online tables are not configured")
    region = os.environ.get("AWS_REGION", "")
    raw_authority = os.environ.get("ADMISSION_AUTHORITY_JSON", "")
    if not region or not raw_authority:
        raise AdmissionDenied("deployment-pinned admission authority and region are required")
    try:
        authority = object_authority(json.loads(raw_authority))
    except ValueError as error:
        raise AdmissionDenied("deployment admission authority is invalid") from error
    config_type: Any = importlib.import_module("botocore.config").Config
    config = config_type(
        retries={"mode": "standard", "total_max_attempts": 2},
        connect_timeout=5,
        read_timeout=10,
    )
    s3: S3Client = boto3.client("s3", region_name=region, config=config)
    dynamodb = boto3.client("dynamodb", region_name=region, config=config)
    ledger = DynamoTaskLedger(dynamodb, table_name)
    glue_config = config_type(
        retries={"mode": "standard", "total_max_attempts": 1},
        connect_timeout=5,
        read_timeout=10,
    )
    account = os.environ.get("EXPECTED_ACCOUNT", "")
    job_name = os.environ.get("GLUE_JOB_NAME", "")
    kms_key = os.environ.get("KMS_KEY_ARN", "")
    if not account or not job_name or not kms_key:
        raise AdmissionDenied("deployment Glue job, account and KMS identities are required")
    return ControlWorker(
        s3=s3,
        ledger=ledger,
        online=DynamoOnlineRuntime(dynamodb, online_table, table_name),
        admission=AWSManagedAdmission(
            s3=s3,
            identity=boto3.client("sts", region_name=region, config=config),
            quotas=boto3.client("service-quotas", region_name=region, config=config),
            budgets=boto3.client("budgets", region_name="us-east-1", config=config),
            authority=authority,
            region=region,
        ),
        glue_launcher=DurableGlueLauncher(
            dynamodb,
            boto3.client("glue", region_name=region, config=glue_config),
            table=table_name,
            job_name=job_name,
            kms_key=kms_key,
            account=account,
        ),
    )


def lambda_handler(event: Mapping[str, Any], _context: Any) -> dict[str, Any]:
    if os.environ.get("FEATUREFORGE_STAGE6_ENABLED") != "true":
        raise RuntimeError("Stage 6 control worker is disabled by default")
    return _lambda_worker().dispatch(event, invocation_id=getattr(_context, "aws_request_id", None))


__all__ = ["ControlWorker", "lambda_handler", "completion_receipt"]
