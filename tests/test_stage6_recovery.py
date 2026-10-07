"""Controlled API fixtures prove collector behavior, never live AWS evidence."""

from __future__ import annotations

import io
from typing import Any

import pytest

from featureforge.canonical import canonical_json, digest
from featureforge.stage6_live import LiveEvidenceError, fingerprint, sha256_bytes
from tools import recover_stage6_readonly as recovery


def lease_fixture() -> dict[str, Any]:
    return {
        "contract": "stage6-lease-snapshot-v1",
        "lease_id": "unit-lease-private-id",
        "owner": recovery.OWNER,
        "source_commit": recovery.SOURCE_COMMIT,
        "acquired_at_epoch": 100,
        "heartbeat_at_epoch": 100,
        "expires_at_epoch": 3400,
    }


class UnitSTS:
    def __init__(self) -> None:
        self.account = recovery.ACCOUNT
        self.arn = f"arn:aws:sts::{self.account}:assumed-role/{recovery.ROLE}/unit-session"
        self.calls: list[str] = []

    def get_caller_identity(self) -> dict[str, str]:
        self.calls.append("get_caller_identity")
        return {"Account": self.account, "Arn": self.arn}


class UnitS3:
    def __init__(self) -> None:
        self.lease = lease_fixture()
        state = {
            "version": 4,
            "terraform_version": "1.9.8",
            "serial": 0,
            "lineage": "unit-state-private-lineage",
            "outputs": {},
            "resources": [],
            "check_results": None,
        }
        self.state = (canonical_json(state) + "\n").encode()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.inventory_overrides: dict[str, Any] = {}
        self.lease_versions = [
            {"Key": recovery.LEASE_KEY, "IsLatest": True, "VersionId": "unit-lease-version"}
        ]
        self.encryption = "AES256"
        self.drift = False
        self.truncate_body = False
        self.oversized = False

    def list_object_versions(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list_object_versions", kwargs))
        if kwargs["Prefix"] == recovery.LEASE_KEY:
            versions = [dict(row) for row in self.lease_versions]
        else:
            versions = [
                {"Key": recovery.STATE_KEY, "IsLatest": True, "VersionId": "unit-state-version"}
            ]
        if self.drift and len(self.calls) > 4:
            versions[0]["VersionId"] += "-changed"
        return {
            "IsTruncated": False,
            "DeleteMarkers": [],
            "Versions": versions,
            **self.inventory_overrides,
        }

    def _response(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        body = (
            canonical_json(self.lease).encode()
            if kwargs["Key"] == recovery.LEASE_KEY
            else self.state
        )
        return {
            "ContentLength": recovery.CAPS[kwargs["Key"]] + 1 if self.oversized else len(body),
            "ServerSideEncryption": self.encryption,
            "VersionId": kwargs["VersionId"],
            "Body": io.BytesIO(body[:-1] if self.truncate_body else body),
        }

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("head_object", kwargs))
        return self._response(kwargs)

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_object", kwargs))
        return self._response(kwargs)


@pytest.fixture
def unit_clients(monkeypatch: pytest.MonkeyPatch) -> tuple[UnitSTS, UnitS3]:
    """Expected byte fingerprints are replaced ONLY for explicit controlled unit fixtures."""
    sts, s3 = UnitSTS(), UnitS3()
    monkeypatch.setattr(recovery, "STATE_SHA256", sha256_bytes(s3.state))
    monkeypatch.setattr(recovery, "LINEAGE_FINGERPRINT", fingerprint("unit-state-private-lineage"))
    monkeypatch.setattr(recovery, "STATE_VERSION_FINGERPRINT", fingerprint("unit-state-version"))
    monkeypatch.setattr(recovery, "LEASE_VERSION_FINGERPRINT", fingerprint("unit-lease-version"))
    return sts, s3


def observe(clients: tuple[UnitSTS, UnitS3], epoch: int = 4000) -> dict[str, Any]:
    return recovery.collect(*clients, executor_commit="a" * 40, observed_at_epoch=epoch)


def test_unit_fixture_read_allowlist_and_historical_receipt(
    unit_clients: tuple[UnitSTS, UnitS3],
) -> None:
    receipt = observe(unit_clients)
    assert receipt["lease_expired_at_observation"]
    assert receipt["lease_single_version"]
    assert receipt["lease_historical_version_latest"]
    assert not receipt["lease_successor_versions_present"]
    assert receipt["historical_evidence_only"]
    assert not receipt["current_execution_authority"]
    assert receipt["lease"]["digest"] == digest(lease_fixture())
    assert receipt["executor_commit"] != receipt["historical_plan_source_commit"]
    assert unit_clients[0].calls == ["get_caller_identity"]
    calls = unit_clients[1].calls
    assert len(calls) == 8
    assert {method for method, _ in calls} == {"list_object_versions", "head_object", "get_object"}
    for method, kwargs in calls:
        assert kwargs["Bucket"] == recovery.BUCKET
        assert kwargs["Prefix" if method == "list_object_versions" else "Key"] in recovery.CAPS
        if method != "list_object_versions":
            assert "VersionId" in kwargs
    public = canonical_json(receipt)
    for private in (
        recovery.ACCOUNT,
        recovery.BUCKET,
        "arn:",
        "unit-lease-private-id",
        "unit-state-private-lineage",
        "unit-state-version",
        "unit-session",
    ):
        assert private not in public
    checksum = receipt.pop("receipt_sha256")
    assert checksum == digest(receipt)


@pytest.mark.parametrize(
    "override",
    [
        {"IsTruncated": True},
        {"DeleteMarkers": [{"Key": recovery.LEASE_KEY}]},
        {"Versions": []},
        {"Versions": [{"Key": "another-key", "VersionId": "v", "IsLatest": True}]},
        {"Versions": [{"Key": recovery.LEASE_KEY, "VersionId": "null", "IsLatest": True}]},
        {"Versions": [{"Key": recovery.LEASE_KEY, "VersionId": "v", "IsLatest": False}]},
        {"Versions": [{"Key": recovery.LEASE_KEY, "VersionId": "v", "IsLatest": True}] * 2},
    ],
)
def test_unit_inventory_controls_fail_closed(
    unit_clients: tuple[UnitSTS, UnitS3], override: dict[str, Any]
) -> None:
    unit_clients[1].inventory_overrides = override
    with pytest.raises(LiveEvidenceError):
        observe(unit_clients)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_commit", "b" * 40),
        ("owner", "another-run"),
        ("heartbeat_at_epoch", 101),
        ("expires_at_epoch", 3700),
        ("acquired_at_epoch", True),
        ("extra", "unexpected"),
    ],
)
def test_unit_lease_controls_fail_closed(
    unit_clients: tuple[UnitSTS, UnitS3], field: str, value: Any
) -> None:
    unit_clients[1].lease[field] = value
    with pytest.raises(LiveEvidenceError):
        observe(unit_clients)


@pytest.mark.parametrize(
    "fault", ["account", "role", "state", "encryption", "drift", "short-body", "oversized"]
)
def test_unit_identity_and_immutability_faults(
    unit_clients: tuple[UnitSTS, UnitS3], fault: str
) -> None:
    sts, s3 = unit_clients
    if fault == "account":
        sts.account = "000000000000"
    elif fault == "role":
        sts.arn = sts.arn.replace(recovery.ROLE, "AnotherRole")
    elif fault == "state":
        s3.state += b" "
    elif fault == "encryption":
        s3.encryption = "aws:kms"
    elif fault == "drift":
        s3.drift = True
    elif fault == "oversized":
        s3.oversized = True
    else:
        s3.truncate_body = True
    with pytest.raises(LiveEvidenceError):
        observe(unit_clients)


def test_unit_cannot_claim_current_authority(unit_clients: tuple[UnitSTS, UnitS3]) -> None:
    assert observe(unit_clients, 101)["current_execution_authority"] is False
    with pytest.raises(LiveEvidenceError, match="predates"):
        observe(unit_clients, 99)
    with pytest.raises(LiveEvidenceError, match="executor"):
        recovery.collect(*unit_clients, executor_commit="invalid", observed_at_epoch=4000)


def test_unit_preserves_historical_lease_after_cas_successor(
    unit_clients: tuple[UnitSTS, UnitS3],
) -> None:
    unit_clients[1].lease_versions = [
        {"Key": recovery.LEASE_KEY, "IsLatest": True, "VersionId": "unit-successor-version"},
        {"Key": recovery.LEASE_KEY, "IsLatest": False, "VersionId": "unit-lease-version"},
    ]
    receipt = observe(unit_clients)
    assert receipt["lease_version_count"] == 2
    assert receipt["lease_historical_version_preserved"]
    assert not receipt["lease_historical_version_latest"]
    assert receipt["lease_successor_versions_present"]
    assert not receipt["lease_single_version"]


def test_unit_state_version_mismatch_fails_before_reads(
    unit_clients: tuple[UnitSTS, UnitS3],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(recovery, "STATE_VERSION_FINGERPRINT", "0" * 64)
    with pytest.raises(LiveEvidenceError, match="state version"):
        observe(unit_clients)
    assert {method for method, _ in unit_clients[1].calls} == {"list_object_versions"}


def test_unit_reader_rejects_other_namespace_without_api_call(
    unit_clients: tuple[UnitSTS, UnitS3],
) -> None:
    with pytest.raises(LiveEvidenceError, match="allowlisted"):
        recovery._inventory(unit_clients[1], "unrelated/project.json")
    assert not unit_clients[1].calls
