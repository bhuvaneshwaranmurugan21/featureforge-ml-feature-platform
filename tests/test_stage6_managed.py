from __future__ import annotations

import base64
import hashlib
import io
import json
from dataclasses import replace
from typing import Any

import pytest

from featureforge.aws_runtime import (
    DynamoTaskLedger,
    RuntimeConflict,
    TaskLedger,
    canonical_payload_bytes,
    put_json_object,
    read_exact_object,
    task_complete_request,
    task_get_request,
    task_put_request,
)
from featureforge.canonical import digest
from featureforge.control_worker import ControlWorker, lambda_handler
from featureforge.managed import (
    AdmissionDenied,
    CostEnvelope,
    CostLine,
    LeaseSnapshot,
    ManagedContractError,
    ManagedRunManifest,
    PlanAuthority,
    ResidualInventory,
    S3ObjectAuthority,
    StalePlanError,
    admit_managed_run,
    completion_receipt,
    glue_start_job_request,
    s3_get_object_request,
    s3_put_object_request,
    stale_variant,
    task_request,
)
from tests.stage6_oracle import cost as oracle_cost
from tests.stage6_oracle import manifest_digest as oracle_manifest_digest

SHA_A = "a" * 64
SHA_B = "b" * 64
COMMIT = "1" * 40
TREE = "2" * 40


def manifest() -> ManagedRunManifest:
    return ManagedRunManifest(
        run_id="run-001",
        source_commit=COMMIT,
        source_tree=TREE,
        region="ap-south-1",
        account_fingerprint=SHA_A,
        namespace="stage6",
        inputs=(S3ObjectAuthority("input-bucket", "source.json", "v1", SHA_B),),
        output_bucket="output-bucket",
        output_prefix="stage6/runs/run-001/",
        artifact_digests=("c" * 64,),
        max_input_rows=100,
        max_output_rows=50,
        max_cost_microusd=25_000_000,
        safety_margin_bps=2_000,
    )


def lease() -> LeaseSnapshot:
    return LeaseSnapshot("lease-1", "run-001", COMMIT, 1_000, 1_100, 1_700)


def inventory(*items: str) -> ResidualInventory:
    return ResidualInventory("stage6", 1_150, tuple(items))


def costs(maximum: int = 25_000_000) -> CostEnvelope:
    return CostEnvelope(
        1_150,
        (CostLine("glue", 1_000_000, 10_000_000, 10_000_000),),
        2_000,
        maximum,
    )


def authority() -> PlanAuthority:
    return PlanAuthority(
        source_commit=COMMIT,
        source_tree=TREE,
        variable_digest="3" * 64,
        provider_lock_digest="4" * 64,
        state_lineage_fingerprint="5" * 64,
        state_serial=7,
        account_fingerprint=SHA_A,
        region="ap-south-1",
        artifact_digests=("c" * 64,),
        inventory_digest="6" * 64,
        lease_digest="7" * 64,
        binary_plan_sha256="8" * 64,
        normalized_plan_sha256="9" * 64,
        created_at_epoch=1_000,
        expires_at_epoch=1_600,
    )


def admission() -> Any:
    return admit_managed_run(
        manifest(),
        observed_account_fingerprint=SHA_A,
        observed_region="ap-south-1",
        lease=lease(),
        inventory=inventory(),
        available_quotas={"glue-runs": 1},
        required_quotas={"glue-runs": 1},
        cost=costs(),
        observed_at_epoch=1_200,
    )


def test_manifest_is_canonical_and_independently_reproducible() -> None:
    value = manifest()
    assert value.manifest_digest == oracle_manifest_digest(value.as_dict())
    assert ManagedRunManifest.from_dict(value.as_dict()) == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("output_prefix", "other/runs/run-001/"),
        ("max_input_rows", 0),
        ("max_cost_microusd", 25_000_001),
        ("safety_margin_bps", 1_999),
        ("safety_margin_bps", 5_001),
        ("artifact_digests", ("bad",)),
    ],
)
def test_manifest_rejects_weakened_bounds(field: str, value: Any) -> None:
    with pytest.raises(ManagedContractError):
        replace(manifest(), **{field: value})


def test_manifest_rejects_conflicting_digest_and_shape() -> None:
    with pytest.raises(ManagedContractError, match="digest"):
        replace(manifest(), manifest_digest="f" * 64)
    value = manifest().as_dict() | {"unexpected": True}
    with pytest.raises(ManagedContractError, match="unexpected"):
        ManagedRunManifest.from_dict(value)


def test_object_authority_rejects_traversal_and_bad_hash() -> None:
    with pytest.raises(ManagedContractError):
        S3ObjectAuthority("bucket", "../secret", "v1", SHA_A)
    with pytest.raises(ManagedContractError):
        S3ObjectAuthority("bucket", "key", "v1", "bad")


def test_lease_and_inventory_fail_closed() -> None:
    assert lease().current_for(manifest(), 1_200)
    assert not lease().current_for(manifest(), 1_700)
    with pytest.raises(ManagedContractError, match="sixty"):
        LeaseSnapshot("lease", "run-001", COMMIT, 1, 1, 3_602)
    with pytest.raises(ManagedContractError, match="non-FeatureForge"):
        ResidualInventory("stage6", 10, ("ledgerguard-resource",))


def test_cost_uses_integer_ceiling_and_oracle_agrees() -> None:
    line = CostLine("requests", 1, 1, 1)
    envelope = CostEnvelope(1, (line,), 2_000, 2)
    assert envelope.worst_case_microusd == oracle_cost(1, 2_000) == 2
    assert envelope.admitted
    assert envelope.as_dict()["currency"] == "USD"
    with pytest.raises(ManagedContractError, match="rounded-up"):
        CostLine("requests", 1, 1, 0)


def test_admission_succeeds_and_binds_every_authority() -> None:
    decision = admission()
    assert decision.decision == "ELIGIBLE"
    assert len(decision.admission_digest) == 64


@pytest.mark.parametrize(
    "change",
    ["account", "region", "lease", "inventory", "quota", "cost", "future-inventory"],
)
def test_admission_negative_controls(change: str) -> None:
    kwargs: dict[str, Any] = {
        "observed_account_fingerprint": SHA_A,
        "observed_region": "ap-south-1",
        "lease": lease(),
        "inventory": inventory(),
        "available_quotas": {"glue-runs": 1},
        "required_quotas": {"glue-runs": 1},
        "cost": costs(),
        "observed_at_epoch": 1_200,
    }
    if change == "account":
        kwargs["observed_account_fingerprint"] = "f" * 64
    elif change == "region":
        kwargs["observed_region"] = "eu-west-1"
    elif change == "lease":
        kwargs["observed_at_epoch"] = 1_700
    elif change == "inventory":
        kwargs["inventory"] = inventory("featureforge-stage6-old")
    elif change == "quota":
        kwargs["available_quotas"] = {"glue-runs": 0}
    elif change == "cost":
        kwargs["cost"] = costs(10_000_000)
    else:
        kwargs["inventory"] = ResidualInventory("stage6", 1_300, ())
    with pytest.raises(AdmissionDenied):
        admit_managed_run(manifest(), **kwargs)


def test_saved_plan_is_current_only_inside_bound_interval() -> None:
    value = authority()
    value.assert_current(value, 1_200)
    assert value.as_dict()["contract"] == "stage6-plan-authority-v1"
    with pytest.raises(StalePlanError, match="validity"):
        value.assert_current(value, 1_600)


@pytest.mark.parametrize(
    "field",
    [
        "source_commit",
        "source_tree",
        "variable_digest",
        "provider_lock_digest",
        "state_lineage_fingerprint",
        "state_serial",
        "account_fingerprint",
        "region",
        "artifact_digests",
        "inventory_digest",
        "lease_digest",
        "created_at_epoch",
        "expires_at_epoch",
    ],
)
def test_saved_plan_rejects_changed_authority(field: str) -> None:
    value = authority()
    with pytest.raises(StalePlanError, match="changed"):
        value.assert_current(stale_variant(value, field), 1_200)


def test_request_builders_are_bounded_and_exact() -> None:
    source = manifest().inputs[0]
    assert s3_get_object_request(source)["VersionId"] == "v1"
    put = s3_put_object_request(
        bucket="output",
        key="stage6/runs/run-001/receipt.json",
        body=b"{}",
        kms_key_arn="arn:aws:kms:ap-south-1:123456789012:key/example",
        expected_bucket_owner="123456789012",
    )
    assert put["ServerSideEncryption"] == "aws:kms"
    assert put["ChecksumAlgorithm"] == "SHA256"
    assert (
        glue_start_job_request("featureforge-stage6", manifest())["Arguments"]["--manifest-digest"]
        == manifest().manifest_digest
    )
    with pytest.raises(ManagedContractError):
        s3_put_object_request(
            bucket="b", key="k", body=b"x", kms_key_arn="k", expected_bucket_owner="bad"
        )


def test_task_ledger_exact_replay_and_conflict() -> None:
    record = task_request("run", "VALIDATE", SHA_A, {"x": 1})
    ledger = TaskLedger()
    assert ledger.begin(record)["state"] == "STARTED"
    completed = ledger.complete("run", "VALIDATE", {"ok": True})
    assert completed["state"] == "COMPLETED"
    assert ledger.complete("run", "VALIDATE", {"ok": True}) == completed
    with pytest.raises(RuntimeConflict):
        ledger.complete("run", "VALIDATE", {"ok": False})
    conflicting = record | {"request_digest": SHA_B}
    with pytest.raises(RuntimeConflict):
        ledger.begin(conflicting)
    with pytest.raises(RuntimeConflict):
        TaskLedger().complete("run", "VALIDATE", {})


def test_dynamodb_task_request_shapes() -> None:
    record = task_request("run", "VALIDATE", SHA_A, {"x": 1})
    assert task_get_request("table", "run", "VALIDATE")["ConsistentRead"] is True
    request = task_put_request("table", record)
    assert request["ConditionExpression"] == "attribute_not_exists(PK) AND attribute_not_exists(SK)"
    assert request["Item"]["request_digest"]["S"] == record["request_digest"]
    complete = task_complete_request("table", record, {"ok": True})
    assert complete["ConditionExpression"].startswith("#state = :started")
    assert complete["ExpressionAttributeValues"][":result"]["S"] == digest({"ok": True})


class FakeDynamo:
    def __init__(self) -> None:
        self.item: dict[str, Any] | None = None

    def get_item(self, **_kwargs: Any) -> dict[str, Any]:
        return {} if self.item is None else {"Item": self.item}

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        if self.item is not None:
            raise AssertionError("test fixture attempted a duplicate put")
        self.item = dict(kwargs["Item"])
        return {}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        if self.item is None:
            raise AssertionError("test fixture attempted update before put")
        self.item = self.item | {
            "result_digest": kwargs["ExpressionAttributeValues"][":result"],
            "result_json": kwargs["ExpressionAttributeValues"][":result_json"],
            "state": kwargs["ExpressionAttributeValues"][":completed"],
        }
        return {"Attributes": self.item}

    def transact_write_items(self, **_kwargs: Any) -> dict[str, Any]:
        return {}


def test_dynamodb_task_ledger_persists_exact_replay() -> None:
    client = FakeDynamo()
    ledger = DynamoTaskLedger(client, "control-table")
    record = task_request("run", "VALIDATE", SHA_A, {"x": 1})
    assert ledger.begin(record) == record
    assert ledger.begin(record) == record
    completed = ledger.complete("run", "VALIDATE", {"ok": True})
    assert completed["state"] == "COMPLETED"
    assert ledger.complete("run", "VALIDATE", {"ok": True}) == completed
    with pytest.raises(RuntimeConflict):
        ledger.complete("run", "VALIDATE", {"ok": False})


def test_execution_owner_survives_new_dynamo_ledger_instance() -> None:
    client = FakeDynamo()
    original = DynamoTaskLedger(client, "control-table")
    owned = task_request("run", "VALIDATE", SHA_A, {"execution_id": "execution-1"})
    original.begin(owned)
    original.complete("run", "VALIDATE", {"execution_id": "execution-1"})
    restarted = DynamoTaskLedger(client, "control-table")
    assert restarted.begin(owned)["result"] == {"execution_id": "execution-1"}
    with pytest.raises(RuntimeConflict, match="conflicting immutable content"):
        restarted.begin(task_request("run", "VALIDATE", SHA_A, {"execution_id": "execution-2"}))
    assert original.get("run", "VALIDATE")["request_digest"] == owned["request_digest"]


class FakeS3:
    def __init__(self, objects: dict[tuple[str, str, str], bytes]) -> None:
        self.objects = objects
        self.writes: list[dict[str, Any]] = []

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        key = (kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"])
        return {"Body": io.BytesIO(self.objects[key]), "VersionId": kwargs["VersionId"]}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.writes.append(kwargs)
        return {
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(kwargs["Body"]).digest()).decode(),
            "VersionId": "written-v1",
        }


def test_exact_s3_read_and_versioned_write() -> None:
    body = b'{"ok":true}'
    source = S3ObjectAuthority("bucket", "k", "v1", hashlib.sha256(body).hexdigest())
    client = FakeS3({("bucket", "k", "v1"): body})
    assert read_exact_object(client, source).body == body
    request = {
        "Body": body,
        "Bucket": "bucket",
        "ExpectedBucketOwner": "123456789012",
        "Key": "out",
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "key",
    }
    assert put_json_object(client, request).version_id == "written-v1"
    assert canonical_payload_bytes({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_bounded_exact_s3_read_rejects_oversize_and_closes_stream() -> None:
    class TrackedBody(io.BytesIO):
        def __init__(self, body: bytes) -> None:
            super().__init__(body)
            self.requested: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.requested.append(size)
            return super().read(size)

    class Boundary(FakeS3):
        def __init__(self, body: bytes) -> None:
            super().__init__({})
            self.body = TrackedBody(body)

        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            return {"Body": self.body, "VersionId": kwargs["VersionId"]}

    body = b"12345"
    source = S3ObjectAuthority("bucket", "k", "v1", hashlib.sha256(body).hexdigest())
    client = Boundary(body)
    with pytest.raises(ManagedContractError, match="exceeds the enforced read byte bound"):
        read_exact_object(client, source, maximum_bytes=4)
    assert client.body.requested == [5]
    assert client.body.closed
    client = Boundary(body)
    assert read_exact_object(client, source, maximum_bytes=5).body == body
    assert client.body.requested == [6]
    assert client.body.closed
    with pytest.raises(ManagedContractError, match="positive integer"):
        read_exact_object(client, source, maximum_bytes=True)


def test_completion_receipt_binds_tasks_and_objects() -> None:
    receipt = completion_receipt(
        manifest=manifest(),
        admission=admission(),
        task_receipts=({"task": "VALIDATE", "state": "COMPLETED"},),
        output_objects=(manifest().inputs[0],),
    )
    assert receipt["contract"] == "stage6-completion-receipt-v1"
    assert len(receipt["receipt_digest"]) == 64


def test_control_worker_validate_replay_and_parity() -> None:
    value = manifest()
    body = json.dumps(value.as_dict(), sort_keys=True, separators=(",", ":")).encode()
    auth = S3ObjectAuthority("manifest", "run.json", "v1", hashlib.sha256(body).hexdigest())
    ledger = TaskLedger()
    worker = ControlWorker(s3=FakeS3({("manifest", "run.json", "v1"): body}), ledger=ledger)
    event = {
        "action": "VALIDATE",
        "manifest": value.as_dict(),
        "manifest_authority": auth.as_dict(),
        "payload": {},
    }
    with pytest.raises(AdmissionDenied, match="admission boundary"):
        worker.dispatch(event)
    assert ledger.get(value.run_id, "VALIDATE") is None


def _worker_event(action: str, payload: dict[str, Any]) -> tuple[ControlWorker, dict[str, Any]]:
    value = manifest()
    manifest_body = json.dumps(value.as_dict(), sort_keys=True, separators=(",", ":")).encode()
    manifest_authority = S3ObjectAuthority(
        "manifest", "run.json", "v1", hashlib.sha256(manifest_body).hexdigest()
    )
    objects = {("manifest", "run.json", "v1"): manifest_body}
    payload_authority = payload.get("online_payload_authority")
    if isinstance(payload_authority, dict):
        online_body = payload.pop("_online_body")
        objects[
            (
                payload_authority["bucket"],
                payload_authority["key"],
                payload_authority["version_id"],
            )
        ] = online_body
    event = {
        "action": action,
        "manifest": value.as_dict(),
        "manifest_authority": manifest_authority.as_dict(),
        "payload": payload,
    }
    return ControlWorker(s3=FakeS3(objects), ledger=TaskLedger()), event


def test_control_worker_glue_completion_fails_closed() -> None:
    worker, event = _worker_event("RECORD_GLUE", {"glue_job_run_id": "jr-1", "state": "SUCCEEDED"})
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)
    worker, event = _worker_event("RECORD_GLUE", {"glue_job_run_id": "jr-2", "state": "FAILED"})
    with pytest.raises(AdmissionDenied):
        worker.dispatch(event)


def test_control_worker_reads_online_payload_by_exact_authority() -> None:
    online_body = json.dumps(
        [{"customer_id": "c1", "feature_name": "f1", "value": 1}],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    online_authority = S3ObjectAuthority(
        "offline", "candidate.json", "v1", hashlib.sha256(online_body).hexdigest()
    )
    worker, event = _worker_event(
        "MATERIALIZE_ONLINE",
        {"online_payload_authority": online_authority.as_dict(), "_online_body": online_body},
    )
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)


def test_control_worker_parity_and_activation_are_guarded() -> None:
    worker, event = _worker_event("PARITY", {"expected_digest": SHA_A, "observed_digest": SHA_A})
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)
    worker, event = _worker_event("PARITY", {"expected_digest": SHA_A, "observed_digest": SHA_B})
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)
    worker, event = _worker_event(
        "ACTIVATE",
        {
            "expected_version": 4,
            "feature_set": "risk",
            "generation_id": "g1",
            "parity_decision": "ELIGIBLE",
        },
    )
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)
    worker, event = _worker_event("ACTIVATE", {"parity_decision": "QUARANTINED"})
    with pytest.raises(AdmissionDenied):
        worker.dispatch(event)


def test_control_worker_completion_receipt_and_unknown_action() -> None:
    worker, event = _worker_event(
        "COMPLETE",
        {
            "admission": {"admission_digest": SHA_A},
            "output_objects": [manifest().inputs[0].as_dict()],
            "task_receipts": [{"task": "VALIDATE", "state": "COMPLETED"}],
        },
    )
    with pytest.raises(AdmissionDenied, match="durable"):
        worker.dispatch(event)
    worker, event = _worker_event("UNKNOWN", {})
    with pytest.raises(ManagedContractError, match="unsupported managed task"):
        worker.dispatch(event)


def test_lambda_handler_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FEATUREFORGE_STAGE6_ENABLED", raising=False)
    with pytest.raises(RuntimeError, match="disabled"):
        lambda_handler({}, None)
