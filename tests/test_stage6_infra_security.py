"""Static checks of the managed security graph; no deployed-service claims."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TF = ROOT / "infra/terraform"


def _text(name: str) -> str:
    return (TF / name).read_text()


def _statement(policy: str, sid: str) -> str:
    match = re.search(rf'sid\s*=\s*"{re.escape(sid)}"', policy)
    assert match, f"missing policy statement {sid}"
    return policy[match.end() :].split("\n  }", 1)[0]


def test_enabled_glue_metrics_have_namespace_and_region_limited_publication_permission() -> None:
    compute = _text("compute.tf")
    assert '"--enable-metrics"' in compute
    block = _statement(_text("iam.tf"), "ExactGlueMetricNamespace")
    assert re.findall(r'"(cloudwatch:[^"\n]+)"', block) == [
        "cloudwatch:PutMetricData",
        "cloudwatch:namespace",
    ]
    assert 'resources = ["*"]' in block
    assert 'values   = ["Glue"]' in block
    assert 'variable = "aws:RequestedRegion"' in block
    assert "values   = [var.aws_region]" in block


def test_glue5_custom_security_log_names_match_the_job_configuration() -> None:
    main = _text("main.tf")
    compute = _text("compute.tf")
    iam = _text("iam.tf")
    expected_locals = {
        "glue_security_name": '"${local.name}-security"',
        "glue_role_name": '"${local.name}-glue"',
        "glue_log_group_prefix": '"/aws-glue/jobs/${local.name}"',
        "glue_log_group_base": (
            '"${local.glue_log_group_prefix}/'
            '${local.glue_security_name}-role/${local.glue_role_name}"'
        ),
    }
    for name, expected in expected_locals.items():
        # Terraform fmt changes alignment, never the exact authority expression.
        assert re.findall(rf"^\s*{name}\s*=\s*(.+)$", main, re.MULTILINE) == [expected]
    for channel in ("error", "output"):
        assert re.search(
            rf'glue_{channel}\s*=\s*"\$\{{local\.glue_log_group_base\}}/{channel}"', main
        )
        group = compute.split(f'resource "aws_cloudwatch_log_group" "glue_{channel}"', 1)[1]
        group = group.split("\n}", 1)[0]
        assert f"name              = local.managed_log_group_names.glue_{channel}" in group
        assert "retention_in_days = 7" in group
        assert "kms_key_id        = aws_kms_key.platform.arn" in group
    assert '"--custom-logGroup-prefix"           = local.glue_log_group_prefix' in compute
    assert "name = local.glue_security_name" in compute
    assert "name               = local.glue_role_name" in iam
    assert (
        "depends_on = [aws_cloudwatch_log_group.glue_error, aws_cloudwatch_log_group.glue_output]"
    ) in compute


def test_log_key_encryption_context_permits_only_exact_managed_groups() -> None:
    main = _text("main.tf")
    policy = _statement(main, "CloudWatchLogsEncryption")
    assert 'test     = "ArnEquals"' in policy
    assert 'variable = "kms:EncryptionContext:aws:logs:arn"' in policy
    assert "for name in values(local.managed_log_group_names)" in policy
    assert "log-group:${name}" in policy
    assert "log-group:/aws/*" not in policy
    assert 'identifiers = ["logs.${var.aws_region}.amazonaws.com"]' in policy
    assert set(re.findall(r'"(kms:[^"\n]+)"', policy)) == {
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*",
        "kms:ReEncrypt*",
        "kms:DescribeKey",
        "kms:EncryptionContext:aws:logs:arn",
    }
    names = main.split("managed_log_group_names = {", 1)[1].split("\n  }", 1)[0]
    assert len(re.findall(r"^    [a-z_]+\s*=", names, re.MULTILINE)) == 4


def test_versioned_buckets_have_a_bounded_cost_horizon_and_exact_destroy_path() -> None:
    main = Path("infra/terraform/main.tf").read_text()
    compute = Path("infra/terraform/compute.tf").read_text()
    assert main.count("force_destroy = true") == 3
    assert 'resource "aws_s3_bucket_lifecycle_configuration" "managed"' in main
    assert 'id     = "stage6-thirty-day-cost-horizon"' in main
    assert "days = 30" in main
    assert "noncurrent_days = 30" in main
    assert "days_after_initiation = 1" in main
    assert "for_each = local.managed_buckets" in main
    assert '"--TempDir"' not in compute


def test_glue_log_permissions_cannot_escape_the_two_retained_groups() -> None:
    iam = _text("iam.tf")
    configure = _statement(iam, "ExactGlueLogGroupConfiguration")
    streams = _statement(iam, "ExactGlueLogStreams")
    key = _statement(iam, "ExactGlueLogKeyAssociation")
    assert 'actions   = ["logs:AssociateKmsKey", "logs:CreateLogGroup"]' in configure
    assert "aws_cloudwatch_log_group.glue_error.arn" in configure
    assert "aws_cloudwatch_log_group.glue_output.arn" in configure
    assert 'actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]' in streams
    assert '"${aws_cloudwatch_log_group.glue_error.arn}:*"' in streams
    assert '"${aws_cloudwatch_log_group.glue_output.arn}:*"' in streams
    assert 'actions   = ["kms:DescribeKey"]' in key
    assert "resources = [aws_kms_key.platform.arn]" in key
    assert 'variable = "kms:ViaService"' in key
    assert 'values   = ["logs.${var.aws_region}.amazonaws.com"]' in key
    assert "log-group:/aws-glue/jobs/*" not in iam
    for statement in (configure, streams):
        assert 'resources = ["*"]' not in statement


def test_active_xray_uses_only_required_service_apis() -> None:
    iam = _text("iam.tf")
    worker = _statement(iam, "WorkerActiveTracing")
    states = _statement(iam, "StateMachineActiveTracing")
    assert set(re.findall(r'"(xray:[^"\n]+)"', worker)) == {
        "xray:PutTraceSegments",
        "xray:PutTelemetryRecords",
    }
    assert set(re.findall(r'"(xray:[^"\n]+)"', states)) == {
        "xray:PutTraceSegments",
        "xray:PutTelemetryRecords",
        "xray:GetSamplingRules",
        "xray:GetSamplingTargets",
    }
    assert 'resources = ["*"]' in worker and 'resources = ["*"]' in states
    assert '"xray:*"' not in iam
    assert 'tracing_config { mode = "Active" }' in _text("compute.tf")
    assert "tracing_configuration { enabled = true }" in _text("compute.tf")


def test_expanded_managed_resource_graph_has_exact_log_and_lifecycle_resources() -> None:
    text = "\n".join(path.read_text() for path in sorted(TF.glob("*.tf")))
    resources = re.findall(r'^resource "([^"]+)" "([^"]+)"', text, re.MULTILINE)
    assert len(resources) == len(set(resources)) == 39
    # Four S3 `managed` resources each expand over the three exact buckets.
    assert text.count("for_each = local.managed_buckets") == 3
    assert text.count("for_each                = local.managed_buckets") == 1
    assert len(resources) + 4 * (3 - 1) == 47
    assert {name for kind, name in resources if kind == "aws_cloudwatch_log_group"} == {
        "control_worker",
        "orchestration",
        "glue_error",
        "glue_output",
    }
