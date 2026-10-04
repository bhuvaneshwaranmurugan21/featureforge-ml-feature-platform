"""Fail-closed authorities for a bounded FeatureForge managed run.

The module is deliberately transport neutral.  It validates immutable identities,
cost/lease/inventory admission and saved-plan freshness without importing an AWS
SDK or performing network I/O.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

from featureforge.canonical import digest

HEX40 = frozenset("0123456789abcdef")
HEX64 = frozenset("0123456789abcdef")
TASKS = frozenset(
    {"VALIDATE", "RECORD_GLUE", "MATERIALIZE_ONLINE", "PARITY", "ACTIVATE", "COMPLETE"}
)
MAX_AUTHORITY_AGE_SECONDS = 3_600
STAGE6_MAX_COST_MICROUSD = 25_000_000
STAGE6_SAFETY_MARGIN_BPS = 2_000
REGION_PATTERN = re.compile(r"^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$")
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,127}$")


class ManagedContractError(ValueError):
    """A managed-run input violates an immutable contract."""


class AdmissionDenied(RuntimeError):
    """A bounded run is not eligible to progress."""


class StalePlanError(RuntimeError):
    """A saved plan no longer binds the current authority."""


def _hex(value: str, length: int, field: str) -> None:
    alphabet = HEX40 if length == 40 else HEX64
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(char not in alphabet for char in value)
    ):
        raise ManagedContractError(f"{field} must be {length} lowercase hexadecimal characters")


def _identity(value: str, field: str, *, maximum: int = 128) -> None:
    if not isinstance(value, str) or not value or len(value) > maximum or value.strip() != value:
        raise ManagedContractError(f"{field} is empty, padded, or too long")


def _integer(value: int, field: str) -> None:
    if type(value) is not int:
        raise ManagedContractError(f"{field} must be an integer without coercion")


@dataclass(frozen=True)
class S3ObjectAuthority:
    bucket: str
    key: str
    version_id: str
    sha256: str

    def __post_init__(self) -> None:
        for field, value in (
            ("bucket", self.bucket),
            ("key", self.key),
            ("version_id", self.version_id),
        ):
            _identity(value, field, maximum=1024)
        if not 3 <= len(self.bucket) <= 63 or "/" in self.bucket:
            raise ManagedContractError("S3 bucket identity is invalid")
        if self.key.startswith("/") or ".." in self.key.split("/"):
            raise ManagedContractError("S3 key must be relative and traversal-free")
        _hex(self.sha256, 64, "sha256")

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ManagedRunManifest:
    run_id: str
    source_commit: str
    source_tree: str
    region: str
    account_fingerprint: str
    namespace: str
    inputs: tuple[S3ObjectAuthority, ...]
    output_bucket: str
    output_prefix: str
    artifact_digests: tuple[str, ...]
    max_input_rows: int
    max_output_rows: int
    max_cost_microusd: int
    safety_margin_bps: int
    manifest_digest: str = ""

    def __post_init__(self) -> None:
        for name in ("max_input_rows", "max_output_rows", "max_cost_microusd", "safety_margin_bps"):
            if type(getattr(self, name)) is not int:
                raise ManagedContractError(f"{name} must be an integer without coercion")
        for field, value in (
            ("run_id", self.run_id),
            ("region", self.region),
            ("namespace", self.namespace),
            ("output_bucket", self.output_bucket),
            ("output_prefix", self.output_prefix),
        ):
            _identity(value, field, maximum=1024)
        _hex(self.source_commit, 40, "source_commit")
        _hex(self.source_tree, 40, "source_tree")
        _hex(self.account_fingerprint, 64, "account_fingerprint")
        if not RUN_ID_PATTERN.fullmatch(self.run_id):
            raise ManagedContractError("run_id must be a lowercase bounded identifier")
        if not REGION_PATTERN.fullmatch(self.region):
            raise ManagedContractError("region must be an explicit AWS region")
        if self.namespace != "stage6":
            raise ManagedContractError("managed namespace must be stage6")
        if not self.inputs:
            raise ManagedContractError("at least one exact S3 input is required")
        input_ids = {(item.bucket, item.key, item.version_id) for item in self.inputs}
        if len(input_ids) != len(self.inputs):
            raise ManagedContractError("duplicate exact input authority")
        if not self.artifact_digests or len(set(self.artifact_digests)) != len(
            self.artifact_digests
        ):
            raise ManagedContractError("artifact digests must be non-empty and unique")
        for artifact_digest in self.artifact_digests:
            _hex(artifact_digest, 64, "artifact_digest")
        if not 1 <= self.max_input_rows <= 1_000_000:
            raise ManagedContractError("max_input_rows is outside the bounded contract")
        if not 1 <= self.max_output_rows <= 500_000:
            raise ManagedContractError("max_output_rows is outside the bounded contract")
        if not 0 < self.max_cost_microusd <= STAGE6_MAX_COST_MICROUSD:
            raise ManagedContractError("max_cost_microusd exceeds Stage 6 authorization")
        if self.safety_margin_bps != STAGE6_SAFETY_MARGIN_BPS:
            raise ManagedContractError("Stage 6 requires a twenty-percent safety margin")
        expected_prefix = f"{self.namespace}/runs/{self.run_id}/"
        if self.output_prefix != expected_prefix:
            raise ManagedContractError("output prefix is not isolated to namespace and run")
        expected = digest(self.body())
        if self.manifest_digest and self.manifest_digest != expected:
            raise ManagedContractError("manifest digest does not bind canonical content")
        if not self.manifest_digest:
            object.__setattr__(self, "manifest_digest", expected)

    def body(self) -> dict[str, Any]:
        return {
            "account_fingerprint": self.account_fingerprint,
            "artifact_digests": list(self.artifact_digests),
            "contract": "stage6-managed-run-v1",
            "inputs": [item.as_dict() for item in self.inputs],
            "max_cost_microusd": self.max_cost_microusd,
            "max_input_rows": self.max_input_rows,
            "max_output_rows": self.max_output_rows,
            "namespace": self.namespace,
            "output_bucket": self.output_bucket,
            "output_prefix": self.output_prefix,
            "region": self.region,
            "run_id": self.run_id,
            "safety_margin_bps": self.safety_margin_bps,
            "source_commit": self.source_commit,
            "source_tree": self.source_tree,
        }

    def as_dict(self) -> dict[str, Any]:
        return self.body() | {"manifest_digest": self.manifest_digest}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ManagedRunManifest:
        expected = set(cls.__dataclass_fields__)
        actual = set(value) - {"contract"}
        if actual != expected:
            raise ManagedContractError("managed manifest has missing or unexpected fields")
        if value.get("contract") != "stage6-managed-run-v1":
            raise ManagedContractError("managed manifest contract mismatch")
        raw_inputs = value.get("inputs")
        if not isinstance(raw_inputs, list):
            raise ManagedContractError("managed inputs must be a list")
        object_fields = {"bucket", "key", "version_id", "sha256"}
        if any(not isinstance(item, Mapping) or set(item) != object_fields for item in raw_inputs):
            raise ManagedContractError("managed input authority has missing or unexpected fields")
        if not isinstance(value["artifact_digests"], list):
            raise ManagedContractError("artifact digests must be a list")
        return cls(
            run_id=value["run_id"],
            source_commit=value["source_commit"],
            source_tree=value["source_tree"],
            region=value["region"],
            account_fingerprint=value["account_fingerprint"],
            namespace=value["namespace"],
            inputs=tuple(S3ObjectAuthority(**item) for item in raw_inputs),
            output_bucket=value["output_bucket"],
            output_prefix=value["output_prefix"],
            artifact_digests=tuple(value["artifact_digests"]),
            max_input_rows=value["max_input_rows"],
            max_output_rows=value["max_output_rows"],
            max_cost_microusd=value["max_cost_microusd"],
            safety_margin_bps=value["safety_margin_bps"],
            manifest_digest=value["manifest_digest"],
        )


@dataclass(frozen=True)
class LeaseSnapshot:
    lease_id: str
    owner: str
    source_commit: str
    acquired_at_epoch: int
    heartbeat_at_epoch: int
    expires_at_epoch: int

    def __post_init__(self) -> None:
        _identity(self.lease_id, "lease_id")
        _identity(self.owner, "owner")
        _hex(self.source_commit, 40, "source_commit")
        for field in ("acquired_at_epoch", "heartbeat_at_epoch", "expires_at_epoch"):
            _integer(getattr(self, field), field)
        if not 0 <= self.acquired_at_epoch <= self.heartbeat_at_epoch < self.expires_at_epoch:
            raise ManagedContractError("lease timestamps are not monotonic")
        if self.expires_at_epoch - self.heartbeat_at_epoch > 3_600:
            raise ManagedContractError("lease heartbeat horizon exceeds sixty minutes")

    def current_for(self, manifest: ManagedRunManifest, observed_at_epoch: int) -> bool:
        _integer(observed_at_epoch, "observed_at_epoch")
        return (
            self.owner == manifest.run_id
            and self.source_commit == manifest.source_commit
            and self.heartbeat_at_epoch <= observed_at_epoch < self.expires_at_epoch
        )

    def as_dict(self) -> dict[str, Any]:
        return {"contract": "stage6-lease-snapshot-v1", **asdict(self)}


@dataclass(frozen=True)
class ResidualInventory:
    namespace: str
    observed_at_epoch: int
    items: tuple[str, ...]

    def __post_init__(self) -> None:
        _identity(self.namespace, "namespace")
        _integer(self.observed_at_epoch, "observed_at_epoch")
        if self.observed_at_epoch < 0:
            raise ManagedContractError("inventory observation time cannot be negative")
        if len(set(self.items)) != len(self.items):
            raise ManagedContractError("inventory contains duplicate resource identities")
        if any(not item.startswith(f"featureforge-{self.namespace}-") for item in self.items):
            raise ManagedContractError("inventory contains a non-FeatureForge namespace item")

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": "stage6-residual-inventory-v1",
            "items": list(self.items),
            "namespace": self.namespace,
            "observed_at_epoch": self.observed_at_epoch,
        }


@dataclass(frozen=True)
class CostLine:
    component: str
    quantity_millionths: int
    unit_cost_microusd: int
    extended_microusd: int

    def __post_init__(self) -> None:
        _identity(self.component, "component")
        for field in ("quantity_millionths", "unit_cost_microusd", "extended_microusd"):
            _integer(getattr(self, field), field)
        if self.quantity_millionths < 0 or self.unit_cost_microusd < 0:
            raise ManagedContractError("cost quantities and rates cannot be negative")
        expected = (self.quantity_millionths * self.unit_cost_microusd + 999_999) // 1_000_000
        if self.extended_microusd != expected:
            raise ManagedContractError("extended cost is not the rounded-up integer product")


@dataclass(frozen=True)
class CostEnvelope:
    pricing_observed_at_epoch: int
    lines: tuple[CostLine, ...]
    safety_margin_bps: int
    maximum_microusd: int

    def __post_init__(self) -> None:
        for field in ("pricing_observed_at_epoch", "safety_margin_bps", "maximum_microusd"):
            _integer(getattr(self, field), field)
        if self.pricing_observed_at_epoch < 0 or not self.lines:
            raise ManagedContractError("pricing authority is absent or empty")
        if (
            self.safety_margin_bps != STAGE6_SAFETY_MARGIN_BPS
            or not 0 < self.maximum_microusd <= STAGE6_MAX_COST_MICROUSD
        ):
            raise ManagedContractError("cost envelope bounds are invalid")

    @property
    def subtotal_microusd(self) -> int:
        return sum(line.extended_microusd for line in self.lines)

    @property
    def worst_case_microusd(self) -> int:
        return (self.subtotal_microusd * (10_000 + self.safety_margin_bps) + 9_999) // 10_000

    @property
    def admitted(self) -> bool:
        return self.worst_case_microusd <= self.maximum_microusd

    def as_dict(self) -> dict[str, Any]:
        return {
            "admitted": self.admitted,
            "contract": "stage6-cost-envelope-v1",
            "currency": "USD",
            "lines": [asdict(line) for line in self.lines],
            "maximum_microusd": self.maximum_microusd,
            "pricing_observed_at_epoch": self.pricing_observed_at_epoch,
            "safety_margin_bps": self.safety_margin_bps,
            "subtotal_microusd": self.subtotal_microusd,
            "worst_case_microusd": self.worst_case_microusd,
        }


@dataclass(frozen=True)
class AdmissionDecision:
    manifest_digest: str
    account_fingerprint: str
    region: str
    lease_digest: str
    inventory_digest: str
    quota_digest: str
    cost_digest: str
    decision: Literal["ELIGIBLE"] = "ELIGIBLE"

    @property
    def admission_digest(self) -> str:
        return digest(asdict(self))


def admit_managed_run(
    manifest: ManagedRunManifest,
    *,
    observed_account_fingerprint: str,
    observed_region: str,
    lease: LeaseSnapshot,
    inventory: ResidualInventory,
    available_quotas: Mapping[str, int],
    required_quotas: Mapping[str, int],
    cost: CostEnvelope,
    observed_at_epoch: int,
) -> AdmissionDecision:
    _integer(observed_at_epoch, "observed_at_epoch")
    _hex(observed_account_fingerprint, 64, "observed_account_fingerprint")
    if observed_account_fingerprint != manifest.account_fingerprint:
        raise AdmissionDenied("AWS account fingerprint mismatch")
    if observed_region != manifest.region:
        raise AdmissionDenied("AWS region mismatch")
    if not lease.current_for(manifest, observed_at_epoch):
        raise AdmissionDenied("lease is not current")
    if inventory.namespace != manifest.namespace or inventory.items:
        raise AdmissionDenied("FeatureForge residual inventory is not empty")
    if (
        not inventory.observed_at_epoch
        <= observed_at_epoch
        <= (inventory.observed_at_epoch + MAX_AUTHORITY_AGE_SECONDS)
    ):
        raise AdmissionDenied("inventory observation is absent, future, or stale")
    if (
        not cost.pricing_observed_at_epoch
        <= observed_at_epoch
        <= (cost.pricing_observed_at_epoch + MAX_AUTHORITY_AGE_SECONDS)
    ):
        raise AdmissionDenied("pricing observation is absent, future, or stale")
    if set(available_quotas) != set(required_quotas):
        raise AdmissionDenied("required quota observation is incomplete")
    if not required_quotas or any(
        type(amount) is not int
        for amount in (*available_quotas.values(), *required_quotas.values())
    ):
        raise AdmissionDenied("quota observations must be nonempty integer authorities")
    if any(amount < 0 for amount in (*available_quotas.values(), *required_quotas.values())):
        raise AdmissionDenied("quota observations cannot be negative")
    if any(available_quotas[name] < amount for name, amount in required_quotas.items()):
        raise AdmissionDenied("one or more required service quotas are unavailable")
    if (
        cost.maximum_microusd != manifest.max_cost_microusd
        or cost.safety_margin_bps != manifest.safety_margin_bps
        or not cost.admitted
    ):
        raise AdmissionDenied("cost envelope exceeds or differs from authorization")
    return AdmissionDecision(
        manifest_digest=manifest.manifest_digest,
        account_fingerprint=observed_account_fingerprint,
        region=observed_region,
        lease_digest=digest(lease.as_dict()),
        inventory_digest=digest(inventory.as_dict()),
        quota_digest=digest(dict(sorted(available_quotas.items()))),
        cost_digest=digest(cost.as_dict()),
    )


@dataclass(frozen=True)
class PlanAuthority:
    source_commit: str
    source_tree: str
    variable_digest: str
    provider_lock_digest: str
    state_lineage_fingerprint: str
    state_serial: int
    account_fingerprint: str
    region: str
    artifact_digests: tuple[str, ...]
    inventory_digest: str
    lease_digest: str
    binary_plan_sha256: str
    normalized_plan_sha256: str
    created_at_epoch: int
    expires_at_epoch: int

    def __post_init__(self) -> None:
        _hex(self.source_commit, 40, "source_commit")
        _hex(self.source_tree, 40, "source_tree")
        for field in ("state_serial", "created_at_epoch", "expires_at_epoch"):
            _integer(getattr(self, field), field)
        for field in (
            "variable_digest",
            "provider_lock_digest",
            "state_lineage_fingerprint",
            "account_fingerprint",
            "inventory_digest",
            "lease_digest",
            "binary_plan_sha256",
            "normalized_plan_sha256",
        ):
            _hex(getattr(self, field), 64, field)
        if self.state_serial < 0 or not self.artifact_digests:
            raise ManagedContractError("plan state serial or artifacts are invalid")
        if len(set(self.artifact_digests)) != len(self.artifact_digests):
            raise ManagedContractError("plan artifact digests must be unique")
        if not REGION_PATTERN.fullmatch(self.region):
            raise ManagedContractError("plan region must be an explicit AWS region")
        for value in self.artifact_digests:
            _hex(value, 64, "artifact_digest")
        if not self.created_at_epoch < self.expires_at_epoch:
            raise ManagedContractError("plan authority expiry is invalid")
        if self.expires_at_epoch - self.created_at_epoch > 3_600:
            raise ManagedContractError("plan authority exceeds sixty minutes")

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": "stage6-plan-authority-v1",
            **asdict(self),
            "artifact_digests": list(self.artifact_digests),
        }

    def assert_current(self, current: PlanAuthority, observed_at_epoch: int) -> None:
        _integer(observed_at_epoch, "observed_at_epoch")
        if not self.created_at_epoch <= observed_at_epoch < self.expires_at_epoch:
            raise StalePlanError("saved plan is outside its validity interval")
        for field in self.__dataclass_fields__:
            if getattr(self, field) != getattr(current, field):
                raise StalePlanError(f"saved plan authority input changed: {field}")


def stale_variant(authority: PlanAuthority, field: str) -> PlanAuthority:
    """Build a valid but different authority for deterministic negative controls."""
    if field == "state_serial":
        return replace(authority, state_serial=authority.state_serial + 1)
    if field == "region":
        return replace(authority, region="eu-west-1")
    if field == "artifact_digests":
        return replace(authority, artifact_digests=("f" * 64,))
    if field == "created_at_epoch":
        return replace(authority, created_at_epoch=authority.created_at_epoch + 1)
    if field == "expires_at_epoch":
        return replace(authority, expires_at_epoch=authority.expires_at_epoch - 1)
    if field not in authority.__dataclass_fields__:
        raise ManagedContractError(f"unknown plan-authority field: {field}")
    value = getattr(authority, field)
    if not isinstance(value, str):
        raise ManagedContractError(f"field cannot be replaced by digest: {field}")
    replacement = "f" * len(value) if value != "f" * len(value) else "e" * len(value)
    changes: dict[str, Any] = {field: replacement}
    return replace(authority, **changes)


def verify_bytes(payload: bytes, expected_sha256: str) -> None:
    _hex(expected_sha256, 64, "expected_sha256")
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ManagedContractError("object bytes do not match exact authority")


def s3_get_object_request(authority: S3ObjectAuthority) -> dict[str, str]:
    return {"Bucket": authority.bucket, "Key": authority.key, "VersionId": authority.version_id}


def s3_put_object_request(
    *, bucket: str, key: str, body: bytes, kms_key_arn: str, expected_bucket_owner: str
) -> dict[str, Any]:
    for field, value in (("bucket", bucket), ("key", key), ("kms_key_arn", kms_key_arn)):
        _identity(value, field, maximum=2048)
    if not expected_bucket_owner.isdigit() or len(expected_bucket_owner) != 12:
        raise ManagedContractError("expected bucket owner must be a 12-digit AWS account")
    return {
        "Body": body,
        "Bucket": bucket,
        "ChecksumAlgorithm": "SHA256",
        "ContentType": "application/json",
        "ExpectedBucketOwner": expected_bucket_owner,
        "Key": key,
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": kms_key_arn,
    }


def glue_start_job_request(job_name: str, manifest: ManagedRunManifest) -> dict[str, Any]:
    _identity(job_name, "job_name")
    return {
        "JobName": job_name,
        "Arguments": {
            "--manifest-digest": manifest.manifest_digest,
            "--max-input-rows": str(manifest.max_input_rows),
            "--max-output-rows": str(manifest.max_output_rows),
            "--run-id": manifest.run_id,
        },
    }


def task_request(
    run_id: str, task: str, manifest_digest: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    _identity(run_id, "run_id")
    if task not in TASKS:
        raise ManagedContractError("unsupported managed task")
    _hex(manifest_digest, 64, "manifest_digest")
    return {
        "contract": "stage6-task-v1",
        "manifest_digest": manifest_digest,
        "request_digest": digest(dict(payload)),
        "run_id": run_id,
        "state": "STARTED",
        "task": task,
    }


def completion_receipt(
    *,
    manifest: ManagedRunManifest,
    admission: AdmissionDecision,
    task_receipts: Sequence[Mapping[str, Any]],
    output_objects: Sequence[S3ObjectAuthority],
) -> dict[str, Any]:
    if not task_receipts or not output_objects:
        raise ManagedContractError("completion requires task and output authorities")
    body: dict[str, Any] = {
        "admission_digest": admission.admission_digest,
        "contract": "stage6-completion-receipt-v1",
        "manifest_digest": manifest.manifest_digest,
        "output_objects": [item.as_dict() for item in output_objects],
        "run_id": manifest.run_id,
        "task_receipts_digest": digest([dict(item) for item in task_receipts]),
    }
    return body | {"receipt_digest": digest(body)}
