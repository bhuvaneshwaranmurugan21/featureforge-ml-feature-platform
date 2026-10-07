from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TF = ROOT / "infra/terraform"


def terraform_text() -> str:
    return "\n".join(path.read_text() for path in sorted(TF.glob("*.tf")))


def test_state_machine_has_real_integrations_and_fail_closed_quarantine() -> None:
    text = (TF / "compute.tf").read_text()
    assert "states:::aws-sdk:glue:getJobRun" in text
    assert 'action                 = "START_GLUE"' in text
    assert text.count("states:::lambda:invoke") >= 5
    assert 'Type  = "Pass"' not in text and 'Type = "Pass"' not in text
    assert 'Default = "Quarantined"' in text
    assert 'state               = "DISABLED"' in text


def test_managed_graph_enforces_security_and_bounded_capacity() -> None:
    text = terraform_text()
    for required in (
        "block_public_acls       = true",
        "point_in_time_recovery",
        'sse_algorithm     = "aws:kms"',
        "reserved_concurrent_executions = 1",
        'worker_type            = "G.1X"',
        "number_of_workers      = 2",
        "timeout                = 15",
        "max_concurrent_runs = var.max_concurrent_runs",
        "include_execution_data = false",
    ):
        assert required in text


def test_plan_role_is_oidc_bound_and_read_only() -> None:
    text = (TF / "iam.tf").read_text()
    assert (
        "repo:bhuvaneshwaranmurugan21@${var.github_repository_owner_id}/"
        "featureforge-ml-feature-platform@${var.github_repository_id}:"
        "environment:${var.github_environment}"
    ) in text
    assert "token.actions.githubusercontent.com:aud" in text
    variables = (TF / "variables.tf").read_text()
    assert 'default     = "276895096"' in variables
    assert 'default     = "1332971230"' in variables
    policy = text.split('data "aws_iam_policy_document" "github_plan_readonly"', 1)[1]
    prohibited = re.compile(
        r'"(?:s3:Put|dynamodb:Put|glue:Start|lambda:Invoke|states:Start|iam:PassRole)'
    )
    assert prohibited.search(policy) is None


def test_toolchain_and_workflow_are_exactly_pinned() -> None:
    versions = (TF / "versions.tf").read_text()
    lock = (TF / ".terraform.lock.hcl").read_text()
    workflow = (ROOT / ".github/workflows/terraform.yml").read_text()
    assert 'required_version = "= 1.9.8"' in versions
    assert 'version = "= 5.100.0"' in versions
    assert 'version     = "5.100.0"' in lock
    assert "actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683" in workflow
    assert "hashicorp/setup-terraform@b9cd54a3c349d3f38e8881555d616ced269862dd" in workflow


def test_read_api_manifest_contains_no_mutations() -> None:
    manifest = json.loads((ROOT / "docs/stage6/aws-read-api-manifest.json").read_text())
    assert manifest["mutating_actions"] == []
    assert "sts:GetCallerIdentity" in manifest["actions"]
    assert not any(
        re.search(r":(?:Put|Create|Delete|Update|Start|Invoke|Stop|Tag|Untag)", action)
        for action in manifest["actions"]
    )


def test_online_table_matches_canonical_stage4_keys_and_exact_scan_scope() -> None:
    text = (TF / "main.tf").read_text()
    online = text.split('resource "aws_dynamodb_table" "online"', 1)[1]
    assert 'hash_key     = "PK"' in online
    assert 'range_key    = "SK"' in online
    assert 'attribute_name = "expires_at"' in online
    assert "ONLINE_TABLE                = aws_dynamodb_table.online.name" in text
    iam = (TF / "iam.tf").read_text()
    scan = iam.split('sid       = "ExactOnlineCandidateReconciliation"', 1)[1].split("}", 1)[0]
    assert 'actions   = ["dynamodb:Scan"]' in scan
    assert "resources = [aws_dynamodb_table.online.arn]" in scan
    assert "aws_dynamodb_table.control.arn" not in scan
