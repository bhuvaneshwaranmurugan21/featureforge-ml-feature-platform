"""Pure boundary proofs for the one-write Stage 6 plan executor."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.canonical import canonical_json
from tools import execute_stage6_plan as executor
from tools import recover_stage6_readonly as recovery


def expired_lease() -> dict[str, object]:
    return {
        "contract": "stage6-lease-snapshot-v1",
        "lease_id": "historical-private-id",
        "owner": executor.RUN_ID,
        "source_commit": executor.PRIOR_SOURCE_COMMIT,
        "acquired_at_epoch": 100,
        "heartbeat_at_epoch": 100,
        "expires_at_epoch": 3_400,
    }


def test_expired_lease_successor_is_exact_source_and_bounded() -> None:
    prior = executor.parse_expired_lease(expired_lease(), observed_at_epoch=4_000)
    assert prior.expires_at_epoch == 3_400
    successor = executor.successor_lease(
        source_commit="b" * 40, observed_at_epoch=4_000, lease_id="c" * 32
    )
    assert successor.owner == executor.RUN_ID
    assert successor.source_commit == "b" * 40
    assert successor.expires_at_epoch - successor.heartbeat_at_epoch == executor.LEASE_SECONDS
    with pytest.raises(executor.PlanExecutionError, match="not expired"):
        executor.parse_expired_lease(expired_lease(), observed_at_epoch=3_399)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("extra", "value"),
        ("owner", "other-run"),
        ("source_commit", "f" * 40),
        ("acquired_at_epoch", True),
        ("contract", "unknown"),
    ],
)
def test_prior_lease_shape_and_identity_fail_closed(field: str, value: object) -> None:
    lease = expired_lease()
    lease[field] = value
    with pytest.raises((executor.PlanExecutionError, ValueError)):
        executor.parse_expired_lease(lease, observed_at_epoch=4_000)


def test_successor_write_is_cas_encrypted_and_checksum_bound() -> None:
    body = (canonical_json(expired_lease()) + "\n").encode()
    request = executor.lease_put_request(
        bucket="private-bucket", previous_etag='"etag"', body=body, expected_owner=executor.ACCOUNT
    )
    assert request["IfMatch"] == '"etag"'
    assert "IfNoneMatch" not in request
    assert request["ServerSideEncryption"] == "AES256"
    assert request["ExpectedBucketOwner"] == executor.ACCOUNT
    assert request["ChecksumAlgorithm"] == "SHA256"
    assert request["ChecksumSHA256"]


def test_successor_is_bound_to_published_historical_lease() -> None:
    assert executor.HISTORICAL_SOURCE_COMMIT == recovery.SOURCE_COMMIT
    assert executor.HISTORICAL_LEASE_VERSION_FINGERPRINT == recovery.LEASE_VERSION_FINGERPRINT
    observation = executor.ROOT / "evidence/stage6/recovery-observation.json"
    value = json.loads(observation.read_text(encoding="utf-8"))
    assert value["lease_object_sha256"] == executor.HISTORICAL_LEASE_OBJECT_SHA256
    assert (
        value["lease_object_version_fingerprint"]
        == executor.HISTORICAL_LEASE_VERSION_FINGERPRINT
    )


def test_retry_is_bound_to_authenticated_expired_successor() -> None:
    observation = executor.ROOT / "evidence/stage6/plan-retry-prior-observation.json"
    value = json.loads(observation.read_text(encoding="utf-8"))
    lease = value["lease"]
    assert value["contract"] == "stage6-plan-retry-prior-observation-v1"
    assert value["aws_writes_executed"] is False
    assert value["lease_expired_at_observation"] is True
    assert value["delete_marker_count"] == 0
    assert lease["source_commit"] == executor.PRIOR_SOURCE_COMMIT
    assert lease["owner"] == executor.RUN_ID
    assert lease["version_count"] == executor.PRIOR_LEASE_VERSION_COUNT
    assert lease["latest_object_sha256"] == executor.PRIOR_LEASE_OBJECT_SHA256
    assert (
        lease["latest_version_fingerprint"]
        == executor.PRIOR_LEASE_VERSION_FINGERPRINT
    )


def test_retry_requires_exact_prior_and_single_successor_version() -> None:
    prior = {"latest": "second", "versions": ("first", "second")}
    executor.validate_authorized_prior_inventory(prior)
    executor.validate_successor_inventory(
        prior,
        {"latest": "third", "versions": ("first", "second", "third")},
        new_version="third",
    )

    with pytest.raises(executor.PlanExecutionError, match="authorized prior versions"):
        executor.validate_authorized_prior_inventory(
            {"latest": "third", "versions": ("first", "second", "third")}
        )

    for changed in (
        {"latest": "second", "versions": ("first", "second", "third")},
        {"latest": "third", "versions": ("first", "second", "third", "fourth")},
        {"latest": "third", "versions": ("first", "third")},
    ):
        with pytest.raises(executor.PlanExecutionError, match="exactly one successor"):
            executor.validate_successor_inventory(prior, changed, new_version="third")


def test_terraform_commands_are_refresh_aware_lock_free_and_plan_only(tmp_path: Path) -> None:
    commands = executor.terraform_commands(
        terraform="terraform",
        backend_file=tmp_path / "backend.hcl",
        variables_file=tmp_path / "vars.json",
        plan_file=tmp_path / "plan.tfplan",
    )
    rendered = "\n".join(" ".join(command) for command in commands)
    assert " plan " in rendered
    assert "-refresh=true" in rendered
    assert "-lock=false" in rendered
    assert " show " in rendered
    assert not any(word in rendered for word in (" apply ", " destroy ", " import "))


def test_exact_fresh_green_qualification_is_required() -> None:
    now = int(datetime(2026, 10, 6, 5, 0, tzinfo=UTC).timestamp())
    value = {
        "name": "Stage 6 AWS Qualification",
        "head_sha": "a" * 40,
        "head_branch": executor.BRANCH,
        "event": "push",
        "status": "completed",
        "conclusion": "success",
        "updated_at": "2026-10-06T04:30:00Z",
    }
    completed = executor.validate_qualification_run(
        value,
        source_commit="a" * 40,
        observed_at_epoch=now,
    )
    assert completed == int(datetime(2026, 10, 6, 4, 30, tzinfo=UTC).timestamp())
    for field, replacement in (
        ("head_sha", "b" * 40),
        ("conclusion", "failure"),
        ("event", "workflow_dispatch"),
        ("updated_at", "2026-10-06T03:00:00Z"),
    ):
        changed = value | {field: replacement}
        with pytest.raises(executor.PlanExecutionError):
            executor.validate_qualification_run(
                changed, source_commit="a" * 40, observed_at_epoch=now
            )


def test_version_inventory_accepts_history_but_rejects_ambiguity() -> None:
    class S3:
        versions = [
            {"Key": executor.LEASE_KEY, "VersionId": "new", "IsLatest": True},
            {"Key": executor.LEASE_KEY, "VersionId": "old", "IsLatest": False},
        ]

        def list_object_versions(self, **kwargs: object) -> dict[str, object]:
            return {"IsTruncated": False, "DeleteMarkers": [], "Versions": self.versions}

    s3 = S3()
    result = executor._version_inventory(s3, "bucket", executor.LEASE_KEY)
    assert result == {"latest": "new", "versions": ("new", "old")}
    s3.versions[1]["IsLatest"] = True
    with pytest.raises(executor.PlanExecutionError, match="exactly one latest"):
        executor._version_inventory(s3, "bucket", executor.LEASE_KEY)
