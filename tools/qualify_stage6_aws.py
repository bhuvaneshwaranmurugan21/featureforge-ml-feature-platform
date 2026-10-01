#!/usr/bin/env python3
"""Execute the enumerated read-only AWS qualification and emit sanitized evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from featureforge.canonical import canonical_json
from featureforge.stage6_live import (
    LiveEvidenceError,
    fingerprint,
    sanitized_identity,
    validate_initial_state,
)

PROJECT = "featureforge-ml-feature-platform"
REGION = "ap-southeast-2"
ROLE_NAME = "FeatureForgeGitHubOidcRole"
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,15}$")
ABSENT_CODES = frozenset(
    {
        "404",
        "NoSuchBucket",
        "NoSuchEntity",
        "ResourceNotFoundException",
        "ResourceNotFound",
    }
)
PRICE_SERVICES = (
    "AWSGlue",
    "AWSLambda",
    "AWSStepFunctions",
    "AmazonDynamoDB",
    "AmazonS3",
    "awskms",
    "AmazonCloudWatch",
)
QUOTA_RULES = {
    "glue": ("concurrent job runs per account", 1.0),
    "lambda": ("concurrent executions", 1.0),
}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _client(service: str, region: str = REGION) -> Any:
    return boto3.client(service, region_name=region)


def _absent_or_present(call: Callable[[], Any], label: str) -> dict[str, str]:
    try:
        call()
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        status = str(error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
        if code in ABSENT_CODES or status == "404":
            return {"resource": label, "status": "ABSENT"}
        raise LiveEvidenceError(f"inventory read failed for {label}: {code or status}") from error
    return {"resource": label, "status": "PRESENT"}


def _verify_backend(account_id: str) -> tuple[dict[str, Any], bytes]:
    s3 = _client("s3")
    bucket = f"featureforge-stage6-tfstate-{account_id}-{REGION}"
    key = "state/stage6/terraform.tfstate"
    location = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
    versioning = s3.get_bucket_versioning(Bucket=bucket).get("Status")
    encryption = s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]
    algorithm = encryption["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    public = s3.get_bucket_policy_status(Bucket=bucket)["PolicyStatus"]["IsPublic"]
    block = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
    ownership = s3.get_bucket_ownership_controls(Bucket=bucket)["OwnershipControls"]["Rules"][0][
        "ObjectOwnership"
    ]
    versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
    exact_versions = [row for row in versions.get("Versions", []) if row.get("Key") == key]
    exact_markers = [row for row in versions.get("DeleteMarkers", []) if row.get("Key") == key]
    if len(exact_versions) != 1 or exact_markers or not exact_versions[0].get("IsLatest"):
        raise LiveEvidenceError("backend state must have exactly one latest version and no markers")
    state_response = s3.get_object(
        Bucket=bucket,
        Key=key,
        VersionId=exact_versions[0]["VersionId"],
    )
    state_bytes = state_response["Body"].read()
    bootstrap = json.loads(Path("evidence/stage6/bootstrap-receipt.json").read_text())
    state = validate_initial_state(
        state_bytes,
        expected_lineage_fingerprint=bootstrap["state_lineage_fingerprint"],
        expected_sha256=bootstrap["state_sha256"],
    )
    checks = {
        "default_encryption_aes256": algorithm == "AES256",
        "owner_enforced": ownership == "BucketOwnerEnforced",
        "public_access_block_complete": all(block.values()),
        "policy_not_public": public is False,
        "region_exact": location == REGION,
        "state_one_version_no_markers": True,
        "versioning_enabled": versioning == "Enabled",
    }
    if not all(checks.values()):
        raise LiveEvidenceError("backend security or region check failed")
    return (
        {
            "bucket_fingerprint": fingerprint(bucket),
            "checks": checks,
            "key_fingerprint": fingerprint(key),
            "state": state,
            "state_version_fingerprint": fingerprint(str(exact_versions[0]["VersionId"])),
        },
        state_bytes,
    )


def _verify_oidc(account_id: str, source_branch: str) -> dict[str, Any]:
    iam = _client("iam")
    role = iam.get_role(RoleName=ROLE_NAME)["Role"]
    trust = role["AssumeRolePolicyDocument"]["Statement"][0]
    equals = trust["Condition"]["StringEquals"]
    subjects = equals["token.actions.githubusercontent.com:sub"]
    if isinstance(subjects, str):
        subjects = [subjects]
    immutable = (
        "repo:bhuvaneshwaranmurugan21@276895096/"
        "featureforge-ml-feature-platform@1332971230:ref:refs/heads/"
    )
    expected = {f"{immutable}main", f"{immutable}{source_branch}"}
    if set(subjects) != expected or equals.get("token.actions.githubusercontent.com:aud") != (
        "sts.amazonaws.com"
    ):
        raise LiveEvidenceError("OIDC trust differs from exact immutable FeatureForge subjects")
    provider_arn = f"arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com"
    provider = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=provider_arn)
    if provider.get("Url") != "token.actions.githubusercontent.com" or provider.get(
        "ClientIDList"
    ) != ["sts.amazonaws.com"]:
        raise LiveEvidenceError("GitHub OIDC provider URL or audience drift")
    return {
        "audience_verified": True,
        "immutable_subjects_verified": True,
        "provider_fingerprint": fingerprint(provider_arn),
        "role_arn_fingerprint": fingerprint(role["Arn"]),
        "role_name": ROLE_NAME,
    }


def _inventory(account_id: str, run_id: str) -> list[dict[str, str]]:
    name = f"featureforge-stage6-{run_id}"
    bucket = f"{name}-{account_id}"
    rows: list[dict[str, str]] = []
    s3 = _client("s3")
    for suffix in ("artifacts", "offline", "evidence"):
        rows.append(
            _absent_or_present(
                lambda suffix=suffix: s3.head_bucket(Bucket=f"{bucket}-{suffix}"),
                f"s3:{suffix}",
            )
        )
    dynamodb = _client("dynamodb")
    for suffix in ("control", "online"):
        rows.append(
            _absent_or_present(
                lambda suffix=suffix: dynamodb.describe_table(TableName=f"{name}-{suffix}"),
                f"dynamodb:{suffix}",
            )
        )
    glue = _client("glue")
    rows.extend(
        [
            _absent_or_present(
                lambda: glue.get_database(Name=f"{name.replace('-', '_')}_offline"),
                "glue:database",
            ),
            _absent_or_present(lambda: glue.get_job(JobName=f"{name}-offline"), "glue:job"),
            _absent_or_present(
                lambda: glue.get_security_configuration(Name=f"{name}-security"),
                "glue:security-configuration",
            ),
        ]
    )
    lam = _client("lambda")
    rows.append(
        _absent_or_present(
            lambda: lam.get_function(FunctionName=f"{name}-control-worker"),
            "lambda:control-worker",
        )
    )
    events = _client("events")
    rows.append(
        _absent_or_present(
            lambda: events.describe_rule(Name=f"{name}-manual-only"),
            "events:manual-only",
        )
    )
    states = _client("stepfunctions")
    rows.append(
        _absent_or_present(
            lambda: states.describe_state_machine(
                stateMachineArn=f"arn:aws:states:{REGION}:{account_id}:stateMachine:{name}-materialization"
            ),
            "states:materialization",
        )
    )
    iam = _client("iam")
    for suffix in ("control-worker", "glue", "states", "events", "github-plan"):
        rows.append(
            _absent_or_present(
                lambda suffix=suffix: iam.get_role(RoleName=f"{name}-{suffix}"),
                f"iam:{suffix}",
            )
        )
    logs = _client("logs")
    log_groups = logs.describe_log_groups(logGroupNamePrefix=f"/aws/lambda/{name}").get(
        "logGroups", []
    ) + logs.describe_log_groups(logGroupNamePrefix=f"/aws/vendedlogs/states/{name}").get(
        "logGroups", []
    )
    rows.append({"resource": "logs:namespaced", "status": "PRESENT" if log_groups else "ABSENT"})
    cloudwatch = _client("cloudwatch")
    alarms = cloudwatch.describe_alarms(AlarmNamePrefix=name).get("MetricAlarms", [])
    rows.append({"resource": "cloudwatch:alarms", "status": "PRESENT" if alarms else "ABSENT"})
    rows.append(
        _absent_or_present(
            lambda: cloudwatch.get_dashboard(DashboardName=f"{name}-managed-run"),
            "cloudwatch:dashboard",
        )
    )
    kms = _client("kms")
    aliases: list[Mapping[str, Any]] = []
    marker: str | None = None
    while True:
        kwargs = {"Marker": marker} if marker else {}
        page = kms.list_aliases(**kwargs)
        aliases.extend(page.get("Aliases", []))
        marker = page.get("NextMarker") if page.get("Truncated") else None
        if not marker:
            break
    rows.append(
        {
            "resource": "kms:alias",
            "status": (
                "PRESENT"
                if any(row.get("AliasName") == f"alias/{name}" for row in aliases)
                else "ABSENT"
            ),
        }
    )
    present = [row["resource"] for row in rows if row["status"] != "ABSENT"]
    if present:
        raise LiveEvidenceError(
            f"residual FeatureForge inventory is not empty: {', '.join(present)}"
        )
    return sorted(rows, key=lambda row: row["resource"])


def _quotas() -> list[dict[str, Any]]:
    servicequotas = _client("service-quotas")
    selected: list[dict[str, Any]] = []
    for service, (needle, minimum) in QUOTA_RULES.items():
        token: str | None = None
        match: Mapping[str, Any] | None = None
        while True:
            kwargs: dict[str, Any] = {"ServiceCode": service, "MaxResults": 100}
            if token:
                kwargs["NextToken"] = token
            page = servicequotas.list_service_quotas(**kwargs)
            for quota in page.get("Quotas", []):
                if needle in str(quota.get("QuotaName", "")).lower():
                    match = quota
                    break
            token = page.get("NextToken")
            if match or not token:
                break
        if match is None or float(match["Value"]) < minimum:
            raise LiveEvidenceError(f"required {service} quota is missing or below {minimum}")
        selected.append(
            {
                "minimum": minimum,
                "quota_code": str(match["QuotaCode"]),
                "quota_name": str(match["QuotaName"]),
                "service": service,
                "value": float(match["Value"]),
            }
        )
    return selected


def _pricing() -> list[dict[str, Any]]:
    pricing = _client("pricing", "us-east-1")
    observations: list[dict[str, Any]] = []
    for service_code in PRICE_SERVICES:
        response = pricing.get_products(
            ServiceCode=service_code,
            Filters=[{"Type": "TERM_MATCH", "Field": "regionCode", "Value": REGION}],
            FormatVersion="aws_v1",
            MaxResults=1,
        )
        products = response.get("PriceList", [])
        if not products:
            raise LiveEvidenceError(f"pricing catalog has no {REGION} product for {service_code}")
        decoded = json.loads(products[0])
        publication = decoded.get("publicationDate")
        if not publication:
            raise LiveEvidenceError(f"pricing catalog publication date missing for {service_code}")
        observations.append(
            {
                "catalog_sample_sha256": _sha(canonical_json(decoded).encode("utf-8")),
                "publication_date": str(publication),
                "service_code": service_code,
            }
        )
    return observations


def _cost_envelope(observed_at_epoch: int) -> dict[str, Any]:
    # Each line is a deliberately conservative upper bound for one bounded run plus 30-day
    # accidental retention. Live Pricing API availability/publication is checked separately.
    bounds_usd = {
        "cloudwatch_logs_metrics_dashboard": Decimal("10.00"),
        "dynamodb_requests_and_storage": Decimal("1.00"),
        "glue_catalog_metadata": Decimal("1.00"),
        "glue_two_dpu_fifteen_minutes": Decimal("1.00"),
        "kms_key_and_requests": Decimal("2.00"),
        "lambda_requests_and_duration": Decimal("0.50"),
        "s3_requests_and_storage": Decimal("1.00"),
        "step_functions_transitions": Decimal("0.10"),
    }
    lines = [
        {"component": component, "upper_bound_microusd": int(amount * 1_000_000)}
        for component, amount in sorted(bounds_usd.items())
    ]
    subtotal = sum(row["upper_bound_microusd"] for row in lines)
    worst_case = (subtotal * 12_000 + 9_999) // 10_000
    maximum = 25_000_000
    if worst_case > maximum:
        raise LiveEvidenceError("conservative cost envelope exceeds the authorized ceiling")
    return {
        "admitted": True,
        "authorization_headroom_microusd": maximum - worst_case,
        "currency": "USD",
        "lines": lines,
        "maximum_microusd": maximum,
        "pricing_observed_at_epoch": observed_at_epoch,
        "safety_margin_bps": 2_000,
        "subtotal_microusd": subtotal,
        "worst_case_microusd": worst_case,
    }


def _financial_visibility(account_id: str) -> dict[str, Any]:
    budgets = _client("budgets", "us-east-1").describe_budgets(AccountId=account_id)
    today = datetime.now(UTC).date()
    start = today.replace(day=1).isoformat()
    end = (today + timedelta(days=1)).isoformat()
    cost = _client("ce", "us-east-1").get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
    )
    if not cost.get("ResultsByTime"):
        raise LiveEvidenceError("Cost Explorer returned no current-month visibility")
    return {
        "account_cost_visibility_verified": True,
        "account_spend_not_used_to_reduce_project_ceiling": True,
        "budget_count": len(budgets.get("Budgets", [])),
        "project_authorization_headroom_verified": True,
    }


def qualify(run_id: str, source_branch: str) -> dict[str, Any]:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise LiveEvidenceError("run ID must be a 3-16 character lowercase identifier")
    observed_at = int(time.time())
    identity = _client("sts").get_caller_identity()
    account_id = str(identity["Account"])
    sanitized = sanitized_identity(account_id, str(identity["Arn"]), ROLE_NAME)
    backend, _ = _verify_backend(account_id)
    inventory = _inventory(account_id, run_id)
    receipt: dict[str, Any] = {
        "account": sanitized,
        "backend": backend,
        "bootstrap_writes_excluded_from_read_only_window": [
            "OIDC_ROLE_CREATE_AND_TRUST_UPDATE",
            "BACKEND_BUCKET_SECURITY_CONFIGURATION",
            "INITIAL_EMPTY_STATE_CONDITIONAL_CREATE",
        ],
        "cost_envelope": _cost_envelope(observed_at),
        "financial_visibility": _financial_visibility(account_id),
        "inventory": {
            "digest": _sha(canonical_json(inventory).encode("utf-8")),
            "empty": True,
            "items": inventory,
        },
        "managed_workload_executed": False,
        "observed_at_epoch": observed_at,
        "observed_at_utc": datetime.fromtimestamp(observed_at, UTC).isoformat(),
        "oidc": _verify_oidc(account_id, source_branch),
        "pricing": _pricing(),
        "project": PROJECT,
        "quotas": _quotas(),
        "region": REGION,
        "run_id": run_id,
        "runtime_resource_mutation_executed": False,
        "stage": 6,
        "terraform_apply_executed": False,
    }
    receipt["receipt_sha256"] = _sha(canonical_json(receipt).encode("utf-8"))
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-branch", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = qualify(args.run_id, args.source_branch)
    except (ClientError, LiveEvidenceError) as error:
        raise SystemExit(f"Stage 6 AWS qualification failed closed: {error}") from None
    args.output.write_text(canonical_json(result) + "\n", encoding="utf-8")
    print(f"Stage 6 read-only AWS qualification passed; receipt={result['receipt_sha256']}")


if __name__ == "__main__":
    main()
