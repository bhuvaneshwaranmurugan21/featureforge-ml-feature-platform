"""Read-only admission from deployment-pinned, immutable approval authority.

An approval must be issued by the trusted qualification pipeline after the cost
and inventory gates pass. This adapter cannot issue approvals, acquire leases,
or turn a request-supplied assertion into authority. Imports perform no I/O.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from featureforge.aws_runtime import S3Client
from featureforge.canonical import canonical_json, digest
from featureforge.managed import (
    AdmissionDenied,
    CostEnvelope,
    CostLine,
    LeaseSnapshot,
    ManagedContractError,
    ManagedRunManifest,
    ResidualInventory,
    S3ObjectAuthority,
    admit_managed_run,
    verify_bytes,
)
from featureforge.stage6_live import budget_headroom

MAX_APPROVAL_BYTES = 128 * 1024
APPROVAL_FIELDS = frozenset(
    {
        "contract",
        "manifest",
        "lease_authority",
        "inventory",
        "cost",
        "quota_requirements",
        "approved_at_epoch",
        "expires_at_epoch",
        "cost_bound_evidence_digest",
    }
)


class IdentityClient(Protocol):
    def get_caller_identity(self) -> Mapping[str, Any]: ...


class QuotaClient(Protocol):
    def get_service_quota(self, **kwargs: Any) -> Mapping[str, Any]: ...


class BudgetClient(Protocol):
    def describe_budgets(self, **kwargs: Any) -> Mapping[str, Any]: ...


def closed(value: Any, fields: frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ManagedContractError(f"{label} has missing or unexpected fields")
    return value


def object_authority(value: Any) -> S3ObjectAuthority:
    row = closed(value, frozenset({"bucket", "key", "version_id", "sha256"}), "object authority")
    return S3ObjectAuthority(**dict(row))


def _integer(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ManagedContractError(f"{label} must be a nonnegative integer without coercion")
    return value


def _read_json(client: S3Client, authority: S3ObjectAuthority, *, latest: bool = False) -> Any:
    request = {"Bucket": authority.bucket, "Key": authority.key}
    if not latest:
        request["VersionId"] = authority.version_id
    response = client.get_object(**request)
    if response.get("VersionId") != authority.version_id:
        raise AdmissionDenied("approval or lease object version changed")
    body = response.get("Body")
    if not isinstance(body, bytes):
        reader = getattr(body, "read", None)
        if not callable(reader):
            raise ManagedContractError("authority body is not readable")
        body = reader(MAX_APPROVAL_BYTES + 1)
    if not isinstance(body, bytes) or len(body) > MAX_APPROVAL_BYTES:
        raise ManagedContractError("authority body exceeds bounded admission size")
    verify_bytes(body, authority.sha256)
    try:
        value = json.loads(body)
    except (ValueError, UnicodeDecodeError) as error:
        raise ManagedContractError("authority body is not JSON") from error
    if body != canonical_json(value).encode():
        raise ManagedContractError("authority body is not canonical JSON")
    return value


class AWSManagedAdmission:
    """Recheck live identity, lease, quota and gross budget before every task.

    Inventory and prices are immutable pipeline observations with a one-hour
    maximum age, under the exclusive lease. Approval issuance is a separate gate;
    an absent or unverified cost-bound proof must never receive a deployment pin.
    """

    def __init__(
        self,
        *,
        s3: S3Client,
        identity: IdentityClient,
        quotas: QuotaClient,
        budgets: BudgetClient,
        authority: S3ObjectAuthority,
        region: str,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.s3 = s3
        self.identity = identity
        self.quotas = quotas
        self.budgets = budgets
        self.authority = authority
        self.region = region
        self.clock = clock or (lambda: int(time.time()))

    def verify(self, manifest: ManagedRunManifest) -> Mapping[str, Any]:
        observed = _integer(self.clock(), "observation time")
        approval = closed(_read_json(self.s3, self.authority), APPROVAL_FIELDS, "approval")
        if approval["contract"] != "stage6-admission-authority-v1":
            raise AdmissionDenied("unsupported admission authority contract")
        approved_manifest = ManagedRunManifest.from_dict(approval["manifest"])
        if approved_manifest != manifest:
            raise AdmissionDenied("run differs from deployment-pinned approval")
        approved = _integer(approval["approved_at_epoch"], "approval time")
        expires = _integer(approval["expires_at_epoch"], "approval expiry")
        if not approved <= observed < expires or not 0 < expires - approved <= 3600:
            raise AdmissionDenied("approval is expired, future, or exceeds one hour")
        bounds_digest = approval["cost_bound_evidence_digest"]
        if (
            not isinstance(bounds_digest, str)
            or len(bounds_digest) != 64
            or any(char not in "0123456789abcdef" for char in bounds_digest)
        ):
            raise AdmissionDenied("approval omits its cost-bound proof identity")
        identity = self.identity.get_caller_identity()
        account = identity.get("Account")
        if not isinstance(account, str) or len(account) != 12 or not account.isdigit():
            raise AdmissionDenied("AWS identity returned an invalid account")
        account_fingerprint = hashlib.sha256(account.encode()).hexdigest()
        if account_fingerprint != manifest.account_fingerprint or self.region != manifest.region:
            raise AdmissionDenied("live AWS account or region differs from approval")
        lease_authority = object_authority(approval["lease_authority"])
        lease_value = closed(
            _read_json(self.s3, lease_authority, latest=True),
            frozenset(
                {
                    "contract",
                    "lease_id",
                    "owner",
                    "source_commit",
                    "acquired_at_epoch",
                    "heartbeat_at_epoch",
                    "expires_at_epoch",
                }
            ),
            "lease",
        )
        if lease_value["contract"] != "stage6-lease-snapshot-v1":
            raise AdmissionDenied("unsupported lease contract")
        lease = LeaseSnapshot(
            **{key: value for key, value in lease_value.items() if key != "contract"}
        )
        inventory_value = closed(
            approval["inventory"],
            frozenset({"contract", "namespace", "observed_at_epoch", "items"}),
            "inventory",
        )
        if inventory_value["contract"] != "stage6-residual-inventory-v1" or not isinstance(
            inventory_value["items"], list
        ):
            raise AdmissionDenied("invalid approved inventory contract")
        inventory = ResidualInventory(
            inventory_value["namespace"],
            inventory_value["observed_at_epoch"],
            tuple(inventory_value["items"]),
        )
        raw_cost = closed(
            approval["cost"],
            frozenset(
                {
                    "admitted",
                    "contract",
                    "currency",
                    "lines",
                    "maximum_microusd",
                    "pricing_observed_at_epoch",
                    "safety_margin_bps",
                    "subtotal_microusd",
                    "worst_case_microusd",
                }
            ),
            "cost",
        )
        if not isinstance(raw_cost["lines"], list):
            raise ManagedContractError("cost lines must be an array")
        cost = CostEnvelope(
            raw_cost["pricing_observed_at_epoch"],
            tuple(
                CostLine(
                    **dict(
                        closed(
                            line,
                            frozenset(
                                {
                                    "component",
                                    "quantity_millionths",
                                    "unit_cost_microusd",
                                    "extended_microusd",
                                }
                            ),
                            "cost line",
                        )
                    )
                )
                for line in raw_cost["lines"]
            ),
            raw_cost["safety_margin_bps"],
            raw_cost["maximum_microusd"],
        )
        if canonical_json(raw_cost) != canonical_json(cost.as_dict()):
            raise AdmissionDenied("approved cost arithmetic or contract differs")
        quota_rows = closed(approval["quota_requirements"], frozenset({"glue", "lambda"}), "quotas")
        available: dict[str, int] = {}
        required: dict[str, int] = {}
        for service, raw in quota_rows.items():
            row = closed(raw, frozenset({"quota_code", "minimum"}), "quota requirement")
            code = row["quota_code"]
            if not isinstance(code, str) or not code.startswith("L-") or len(code) > 32:
                raise ManagedContractError("quota code is invalid")
            minimum = _integer(row["minimum"], "quota minimum")
            if minimum < 1:
                raise AdmissionDenied("quota minimum must cover a managed run")
            response = self.quotas.get_service_quota(ServiceCode=service, QuotaCode=code)
            quota = response.get("Quota", {})
            if quota.get("ServiceCode") != service or quota.get("QuotaCode") != code:
                raise AdmissionDenied("quota response differs from requested authority")
            try:
                value = Decimal(str(quota["Value"]))
            except (KeyError, InvalidOperation) as error:
                raise AdmissionDenied("quota value is missing or invalid") from error
            if not value.is_finite() or value < 0:
                raise AdmissionDenied("quota value is nonfinite or negative")
            available[service] = int(value)
            required[service] = minimum
        decision = admit_managed_run(
            manifest,
            observed_account_fingerprint=account_fingerprint,
            observed_region=self.region,
            lease=lease,
            inventory=inventory,
            available_quotas=available,
            required_quotas=required,
            cost=cost,
            observed_at_epoch=observed,
        )
        budget_rows: list[Mapping[str, Any]] = []
        token: str | None = None
        tokens: set[str] = set()
        for _ in range(100):
            request: dict[str, Any] = {
                "AccountId": account,
                "MaxResults": 100,
                "ShowFilterExpression": True,
            }
            if token:
                request["NextToken"] = token
            page = self.budgets.describe_budgets(**request)
            budget_rows.extend(page.get("Budgets", []))
            token = page.get("NextToken")
            if not token:
                break
            if not isinstance(token, str) or token in tokens:
                raise AdmissionDenied("budget pagination is invalid or cyclic")
            tokens.add(token)
        else:
            raise AdmissionDenied("budget observation exceeds bounded page count")
        headroom = budget_headroom(
            budget_rows,
            observed_at_epoch=observed,
            worst_case_microusd=cost.worst_case_microusd,
        )
        return asdict(decision) | {
            "admission_digest": decision.admission_digest,
            "run_id": manifest.run_id,
            "approval_authority_digest": digest(self.authority.as_dict()),
            "cost_bound_evidence_digest": bounds_digest,
            "budget_headroom_digest": digest(headroom),
        }

    def verify_current(
        self, manifest: ManagedRunManifest, prior_receipt: Mapping[str, Any]
    ) -> None:
        current = self.verify(manifest)
        for field in (
            "manifest_digest",
            "run_id",
            "approval_authority_digest",
            "cost_bound_evidence_digest",
        ):
            if current[field] != prior_receipt.get(field):
                raise AdmissionDenied(f"durable admission authority changed: {field}")


__all__ = ["AWSManagedAdmission", "object_authority"]
