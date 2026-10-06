from __future__ import annotations

import hashlib
import json

import pytest
from botocore.exceptions import ClientError

from featureforge.stage6_live import (
    LiveEvidenceError,
    fingerprint,
    normalize_terraform_plan,
    sanitized_identity,
    validate_initial_state,
    validate_lease,
)
from tools.qualify_stage6_aws import PRICE_SERVICES, _absent_or_present

COMMIT = "a" * 40


def _client_error(code: str, status: int = 400) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "controlled test error"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "ControlledRead",
    )


@pytest.mark.parametrize("code", ["EntityNotFoundException", "StateMachineDoesNotExist"])
def test_service_specific_missing_resource_codes_are_absent(code: str) -> None:
    def missing_resource() -> None:
        raise _client_error(code)

    assert _absent_or_present(missing_resource, "service:resource") == {
        "resource": "service:resource",
        "status": "ABSENT",
    }


def test_inventory_read_does_not_hide_non_absence_errors() -> None:
    def denied_read() -> None:
        raise _client_error("AccessDeniedException", 403)

    with pytest.raises(LiveEvidenceError, match="AccessDeniedException"):
        _absent_or_present(denied_read, "glue:database")


def test_step_functions_uses_the_live_aws_price_list_service_code() -> None:
    assert "AmazonStates" in PRICE_SERVICES
    assert "AWSStepFunctions" not in PRICE_SERVICES


def test_initial_state_binds_bytes_and_lineage() -> None:
    state = {
        "check_results": None,
        "lineage": "lineage-1",
        "outputs": {},
        "resources": [],
        "serial": 0,
        "terraform_version": "1.9.8",
        "version": 4,
    }
    body = (json.dumps(state, separators=(",", ":"), sort_keys=True) + "\n").encode()
    result = validate_initial_state(
        body,
        expected_lineage_fingerprint=fingerprint("lineage-1"),
        expected_sha256=hashlib.sha256(body).hexdigest(),
    )
    assert result["serial"] == 0
    with pytest.raises(LiveEvidenceError, match="checksum"):
        validate_initial_state(
            body + b" ",
            expected_lineage_fingerprint=fingerprint("lineage-1"),
            expected_sha256=hashlib.sha256(body).hexdigest(),
        )


def test_lease_requires_exact_commit_owner_and_current_window() -> None:
    lease = {
        "contract": "stage6-lease-snapshot-v1",
        "lease_id": "lease-1",
        "owner": "s6-plan-20260930",
        "source_commit": COMMIT,
        "acquired_at_epoch": 100,
        "heartbeat_at_epoch": 110,
        "expires_at_epoch": 600,
    }
    assert (
        validate_lease(
            lease,
            source_commit=COMMIT,
            owner="s6-plan-20260930",
            observed_at_epoch=200,
        )["source_commit"]
        == COMMIT
    )
    with pytest.raises(LiveEvidenceError, match="not current"):
        validate_lease(
            lease,
            source_commit=COMMIT,
            owner="s6-plan-20260930",
            observed_at_epoch=600,
        )


def _plan(actions: list[str] | None = None) -> dict[str, object]:
    rows = [
        (
            "aws_cloudwatch_event_rule.materialization",
            "aws_cloudwatch_event_rule",
            {"name": "featureforge-stage6-s6-plan-20260930-manual-only", "state": "DISABLED"},
        ),
        (
            "aws_lambda_function.control_worker",
            "aws_lambda_function",
            {
                "function_name": "featureforge-stage6-s6-plan-20260930-control-worker",
                "reserved_concurrent_executions": 1,
            },
        ),
        (
            "aws_glue_job.offline",
            "aws_glue_job",
            {
                "name": "featureforge-stage6-s6-plan-20260930-offline",
                "execution_property": [{"max_concurrent_runs": 1}],
                "number_of_workers": 2,
                "timeout": 15,
                "worker_type": "G.1X",
            },
        ),
        (
            "aws_dynamodb_table.control",
            "aws_dynamodb_table",
            {
                "name": "featureforge-stage6-s6-plan-20260930-control",
                "point_in_time_recovery": [{"enabled": True}],
            },
        ),
        (
            "aws_dynamodb_table.online",
            "aws_dynamodb_table",
            {
                "name": "featureforge-stage6-s6-plan-20260930-online",
                "point_in_time_recovery": [{"enabled": True}],
                "ttl": [{"enabled": True}],
            },
        ),
        *[
            (
                f'aws_s3_bucket_lifecycle_configuration.managed["{name}"]',
                "aws_s3_bucket_lifecycle_configuration",
                {
                    "rule": [
                        {
                            "abort_incomplete_multipart_upload": [
                                {"days_after_initiation": 1}
                            ],
                            "expiration": [{"days": 30}],
                            "id": "stage6-thirty-day-cost-horizon",
                            "noncurrent_version_expiration": [{"noncurrent_days": 30}],
                            "status": "Enabled",
                        }
                    ]
                },
            )
            for name in ("artifacts", "evidence", "offline")
        ],
    ]
    return {
        "format_version": "1.2",
        "terraform_version": "1.9.8",
        "resource_changes": [
            {
                "address": address,
                "name": address.rsplit(".", 1)[1],
                "type": resource_type,
                "change": {"actions": actions or ["create"], "after": after},
            }
            for address, resource_type, after in rows
        ],
    }


def test_plan_normalization_is_sanitized_and_fail_closed() -> None:
    normalized = normalize_terraform_plan(_plan(), run_id="s6-plan-20260930")
    assert normalized["resource_count"] == 8
    assert normalized["action_counts"] == {"create": 8}
    assert all(normalized["checks"].values())
    assert len(normalized["normalized_plan_sha256"]) == 64
    with pytest.raises(LiveEvidenceError, match="not allowlisted"):
        normalize_terraform_plan(_plan(["delete", "create"]), run_id="s6-plan-20260930")


def test_plan_normalization_accepts_exact_managed_glue_log_namespace() -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    resource_changes.append(
        {
            "address": "aws_cloudwatch_log_group.glue_error",
            "name": "glue_error",
            "type": "aws_cloudwatch_log_group",
            "change": {
                "actions": ["create"],
                "after": {
                    "name": (
                        "/aws-glue/jobs/featureforge-stage6-s6-plan-20260930/"
                        "featureforge_stage6_s6_plan_20260930_glue_security-role/"
                        "featureforge-stage6-s6-plan-20260930-glue-role/error"
                    )
                },
            },
        }
    )

    normalized = normalize_terraform_plan(plan, run_id="s6-plan-20260930")

    assert normalized["resource_count"] == 9
    assert normalized["action_counts"] == {"create": 9}


@pytest.mark.parametrize(
    "name",
    [
        "/aws-glue/jobs/unrelated/error",
        "/aws-glue/jobs/featureforge-stage6-s6-plan-20260930-escape/error",
        "/aws-glue/jobs/featureforge-stage6-s6-plan-20260930",
    ],
)
def test_plan_normalization_rejects_glue_log_namespace_escape(name: str) -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    resource_changes.append(
        {
            "address": "aws_cloudwatch_log_group.glue_error",
            "name": "glue_error",
            "type": "aws_cloudwatch_log_group",
            "change": {"actions": ["create"], "after": {"name": name}},
        }
    )

    with pytest.raises(LiveEvidenceError, match="escapes FeatureForge namespace"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")


@pytest.mark.parametrize(
    "execution_property",
    [
        [],
        [{"max_concurrent_runs": 2}],
        [{"max_concurrent_runs": 1}, {"max_concurrent_runs": 1}],
    ],
)
def test_plan_normalization_rejects_unbounded_glue_execution_property(
    execution_property: list[dict[str, int]],
) -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    glue = next(row for row in resource_changes if row["address"] == "aws_glue_job.offline")
    glue["change"]["after"]["execution_property"] = execution_property
    with pytest.raises(LiveEvidenceError, match="glue_concurrency_one"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "unreviewed-rule"),
        ("status", "Disabled"),
        ("expiration", [{"days": 31}]),
        ("noncurrent_version_expiration", [{"noncurrent_days": 31}]),
        ("abort_incomplete_multipart_upload", [{"days_after_initiation": 2}]),
    ],
)
def test_plan_normalization_rejects_lifecycle_cost_horizon_drift(
    field: str, value: object
) -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    lifecycle = next(
        row
        for row in resource_changes
        if row["address"]
        == 'aws_s3_bucket_lifecycle_configuration.managed["artifacts"]'
    )
    lifecycle["change"]["after"]["rule"][0][field] = value
    with pytest.raises(LiveEvidenceError, match="s3_lifecycle_cost_horizon"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")


def test_plan_normalization_requires_all_three_lifecycle_resources() -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    plan["resource_changes"] = [
        row
        for row in resource_changes
        if row["address"]
        != 'aws_s3_bucket_lifecycle_configuration.managed["evidence"]'
    ]
    with pytest.raises(LiveEvidenceError, match="s3_lifecycle_cost_horizon"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")


def test_plan_normalization_rejects_extra_or_ambiguous_lifecycle_resources() -> None:
    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    extra = dict(resource_changes[-1])
    extra["address"] = 'aws_s3_bucket_lifecycle_configuration.managed["unreviewed"]'
    resource_changes.append(extra)
    with pytest.raises(LiveEvidenceError, match="s3_lifecycle_cost_horizon"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")

    plan = _plan()
    resource_changes = plan["resource_changes"]
    assert isinstance(resource_changes, list)
    lifecycle = next(
        row
        for row in resource_changes
        if row["address"]
        == 'aws_s3_bucket_lifecycle_configuration.managed["offline"]'
    )
    lifecycle["change"]["after"]["rule"].append(
        dict(lifecycle["change"]["after"]["rule"][0])
    )
    with pytest.raises(LiveEvidenceError, match="s3_lifecycle_cost_horizon"):
        normalize_terraform_plan(plan, run_id="s6-plan-20260930")


def test_identity_is_fingerprinted_and_role_bound() -> None:
    result = sanitized_identity(
        "123456789012",
        "arn:aws:sts::123456789012:assumed-role/FeatureForgeGitHubOidcRole/run",
        "FeatureForgeGitHubOidcRole",
    )
    assert "123456789012" not in json.dumps(result)
    with pytest.raises(LiveEvidenceError, match="authorized"):
        sanitized_identity(
            "123456789012",
            "arn:aws:sts::123456789012:assumed-role/OtherRole/run",
            "FeatureForgeGitHubOidcRole",
        )
