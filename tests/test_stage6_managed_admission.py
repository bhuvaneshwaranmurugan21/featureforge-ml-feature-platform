"""Local read-boundary proofs; these do not qualify a production approval."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from featureforge.canonical import canonical_json
from featureforge.managed import AdmissionDenied, ManagedContractError, S3ObjectAuthority
from featureforge.managed_admission import AWSManagedAdmission
from featureforge.stage6_live import LiveEvidenceError
from tests.test_stage6_managed import costs, inventory, lease, manifest


class ReadBoundary:
    def __init__(self) -> None:
        self.account = "123456789012"
        self.version = "v1"
        self.quota: Any = 10
        self.requests: list[dict[str, Any]] = []
        self.objects: dict[str, bytes] = {}
        self.budget: dict[str, Any] = {
            "BudgetName": "gross-account",
            "BudgetType": "COST",
            "TimeUnit": "MONTHLY",
            "BudgetLimit": {"Unit": "USD", "Amount": "100"},
            "TimePeriod": {
                "Start": datetime.fromtimestamp(1000, UTC),
                "End": datetime.fromtimestamp(2000, UTC),
            },
            "CostTypes": {"IncludeCredit": False, "IncludeRefund": False},
            "CalculatedSpend": {
                "ActualSpend": {"Unit": "USD", "Amount": "1"},
                "ForecastedSpend": {"Unit": "USD", "Amount": "2"},
            },
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        return {"VersionId": self.version, "Body": self.objects[kwargs["Key"]]}

    def get_caller_identity(self) -> dict[str, Any]:
        return {"Account": self.account}

    def get_service_quota(self, **kwargs: Any) -> dict[str, Any]:
        return {"Quota": kwargs | {"Value": self.quota}}

    def describe_budgets(self, **kwargs: Any) -> dict[str, Any]:
        return {"Budgets": [self.budget]}


def setup_admission() -> tuple[Any, ReadBoundary, dict[str, Any]]:
    boundary = ReadBoundary()
    run = replace(
        manifest(),
        account_fingerprint=hashlib.sha256(boundary.account.encode()).hexdigest(),
        manifest_digest="",
    )
    lease_bytes = canonical_json(lease().as_dict()).encode()
    boundary.objects["leases/featureforge/stage6.json"] = lease_bytes
    lease_pin = S3ObjectAuthority(
        "state-bucket",
        "leases/featureforge/stage6.json",
        "v1",
        hashlib.sha256(lease_bytes).hexdigest(),
    )
    approval = {
        "contract": "stage6-admission-authority-v1",
        "manifest": run.as_dict(),
        "lease_authority": lease_pin.as_dict(),
        "inventory": inventory().as_dict(),
        "cost": costs().as_dict(),
        "quota_requirements": {
            service: {"quota_code": "L-TEST", "minimum": 1} for service in ("glue", "lambda")
        },
        "approved_at_epoch": 1150,
        "expires_at_epoch": 1600,
        "cost_bound_evidence_digest": "d" * 64,
    }
    return run, boundary, approval


def adapter(
    boundary: ReadBoundary, approval: dict[str, Any], now: int = 1200
) -> AWSManagedAdmission:
    body = canonical_json(approval).encode()
    boundary.objects["admission/run.json"] = body
    pin = S3ObjectAuthority(
        "artifact-bucket", "admission/run.json", "v1", hashlib.sha256(body).hexdigest()
    )
    return AWSManagedAdmission(
        s3=boundary,
        identity=boundary,
        quotas=boundary,
        budgets=boundary,
        authority=pin,
        region="ap-south-1",
        clock=lambda: now,
    )


def test_admission_rechecks_pinned_approval_and_latest_lease() -> None:
    run, boundary, approval = setup_admission()
    admission = adapter(boundary, approval)
    receipt = admission.verify(run)
    admission.verify_current(run, receipt)
    assert receipt["manifest_digest"] == run.manifest_digest
    assert receipt["cost_bound_evidence_digest"] == "d" * 64
    assert boundary.requests[0]["VersionId"] == "v1"
    assert "VersionId" not in boundary.requests[1]
    boundary.version = "v2"
    with pytest.raises(AdmissionDenied, match="version changed"):
        admission.verify_current(run, receipt)


@pytest.mark.parametrize("now", [1149, 1600, 2000])
def test_expired_or_future_approval_denied(now: int) -> None:
    run, boundary, approval = setup_admission()
    with pytest.raises(AdmissionDenied, match="approval is expired"):
        adapter(boundary, approval, now).verify(run)


@pytest.mark.parametrize("value", [True, 1.5, "1150", None])
def test_approval_time_has_no_numeric_coercion(value: Any) -> None:
    run, boundary, approval = setup_admission()
    approval["approved_at_epoch"] = value
    with pytest.raises(ManagedContractError, match="without coercion"):
        adapter(boundary, approval).verify(run)


@pytest.mark.parametrize("quota", [0, float("nan"), float("inf"), -1])
def test_unavailable_or_nonfinite_quota_denied(quota: Any) -> None:
    run, boundary, approval = setup_admission()
    boundary.quota = quota
    with pytest.raises(AdmissionDenied):
        adapter(boundary, approval).verify(run)


def test_source_binding_and_live_account_are_required() -> None:
    run, boundary, approval = setup_admission()
    admission = adapter(boundary, approval)
    changed = replace(run, source_commit="f" * 40, manifest_digest="")
    with pytest.raises(AdmissionDenied, match="differs from deployment"):
        admission.verify(changed)
    boundary.account = "999999999999"
    with pytest.raises(AdmissionDenied, match="account or region"):
        admission.verify(run)


def test_budget_and_cost_arithmetic_cannot_be_weakened() -> None:
    run, boundary, approval = setup_admission()
    approval["cost"]["worst_case_microusd"] = 1
    with pytest.raises(AdmissionDenied, match="arithmetic"):
        adapter(boundary, approval).verify(run)
    approval["cost"] = costs().as_dict()
    boundary.budget["CostTypes"]["IncludeCredit"] = True
    with pytest.raises(LiveEvidenceError, match="no applicable"):
        adapter(boundary, approval).verify(run)


def test_recheck_requires_original_durable_authority() -> None:
    run, boundary, approval = setup_admission()
    admission = adapter(boundary, approval)
    receipt = dict(admission.verify(run))
    receipt["cost_bound_evidence_digest"] = "e" * 64
    with pytest.raises(AdmissionDenied, match="durable admission authority changed"):
        admission.verify_current(run, receipt)
