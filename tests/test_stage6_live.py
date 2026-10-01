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
                "max_concurrent_runs": 1,
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
    assert normalized["resource_count"] == 5
    assert normalized["action_counts"] == {"create": 5}
    assert all(normalized["checks"].values())
    assert len(normalized["normalized_plan_sha256"]) == 64
    with pytest.raises(LiveEvidenceError, match="not allowlisted"):
        normalize_terraform_plan(_plan(["delete", "create"]), run_id="s6-plan-20260930")


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
