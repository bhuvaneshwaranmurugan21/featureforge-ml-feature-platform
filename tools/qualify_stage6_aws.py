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
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from featureforge.canonical import canonical_json
from featureforge.stage6_live import (
    LiveEvidenceError,
    budget_headroom,
    fingerprint,
    ondemand_dimensions,
    priced_cost_envelope,
    sanitized_identity,
    validate_initial_state,
)

PROJECT = "featureforge-ml-feature-platform"
REGION = "ap-southeast-2"
ROLE_NAME = "FeatureForgeGitHubOidcRole"
EXPECTED_ACCOUNT_FINGERPRINT = fingerprint("857229544428")
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,15}$")
ABSENT_CODES = frozenset(
    {
        "404",
        "EntityNotFoundException",
        "NoSuchBucket",
        "NoSuchEntity",
        "ResourceNotFoundException",
        "ResourceNotFound",
        "StateMachineDoesNotExist",
    }
)
PRICE_SERVICES = (
    "AWSGlue",
    "AWSLambda",
    "AmazonStates",
    "AmazonDynamoDB",
    "AmazonS3",
    "awskms",
    "AmazonCloudWatch",
    "AWSXRay",
)
QUOTA_RULES = {
    "glue": ("concurrent job runs per account", 1.0),
    "lambda": ("concurrent executions", 1.0),
}
COST_PROFILE_CONTRACT = "stage6-cost-workload-profile-v2"
COST_PROFILE_SCOPE = "ONE_SEPARATELY_AUTHORIZED_EXECUTION_AND_THIRTY_DAY_COST_HORIZON"
COST_EXPLORER_PRICE_URL = (
    "https://aws.amazon.com/aws-cost-management/aws-cost-explorer/pricing/"
)
EXPECTED_COST_QUANTITIES = {
    "glue-dpu-hours": 1_500_000,
    "lambda-gb-seconds": 4_200_000_000,
    "lambda-requests": 28_000_000,
    "states-transitions": 2_000_000_000,
    "dynamodb-write-units": 250_000_000_000,
    "dynamodb-read-units": 250_000_000_000,
    "dynamodb-storage": 2_000_000,
    "dynamodb-pitr": 2_000_000,
    "s3-storage": 2_000_000,
    "s3-write-requests": 250_000_000_000,
    "s3-read-requests": 250_000_000_000,
    "kms-key": 1_000_000,
    "kms-requests": 250_000_000_000,
    "logs-ingestion": 1_000_000,
    "logs-storage": 1_000_000,
    "metric-alarms": 3_000_000,
    "custom-metrics": 10_000_000,
    "dashboard": 1_000_000,
    "xray-recording": 250_000_000,
    "xray-retrieval": 250_000_000,
    "glue-catalog-storage": 1_000_000_000,
    "glue-catalog-requests": 1_000_000_000,
    "cost-explorer-requests": 1_000_000,
}
EXPECTED_KMS_REQUEST_SELECTOR = {
    "service_code": "awskms",
    "units": ["Requests"],
    "usage_pattern": "^ap-southeast-2-KMS-Requests(?: |$)",
}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _client(service: str, region: str = REGION) -> Any:
    return boto3.client(
        service,
        region_name=region,
        config=Config(
            retries={"mode": "standard", "total_max_attempts": 1},
            connect_timeout=5,
            read_timeout=10,
        ),
    )


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


def _pricing(observed_at_epoch: int, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    pricing = _client("pricing", "us-east-1")
    observations: list[dict[str, Any]] = []
    missing_rates: list[str] = []
    for line in profile["lines"]:
        if line["service_code"] != "DOCUMENTED_FIXED_RATE":
            continue
        if (
            line.get("component") != "cost-explorer-requests"
            or line.get("units") != ["Request"]
            or line.get("usage_pattern") != "PrimaryBillingViewRequest"
            or line.get("fixed_unit_cost_microusd") != 10_000
            or line.get("price_authority_url") != COST_EXPLORER_PRICE_URL
            or line.get("price_authority_observed_date") != "2026-10-05"
        ):
            raise LiveEvidenceError("Cost Explorer fixed price authority drift")
        authority_date = datetime.strptime(
            line["price_authority_observed_date"], "%Y-%m-%d"
        ).date()
        observation_date = datetime.fromtimestamp(observed_at_epoch, UTC).date()
        age_days = (observation_date - authority_date).days
        if not 0 <= age_days <= 7:
            raise LiveEvidenceError("Cost Explorer published price authority is stale or future")
        observations.append(
            {
                "component": "cost-explorer-requests",
                "service_code": "AWSCostExplorer",
                "region": "ACCOUNT_GLOBAL",
                "unit_cost_microusd": 10_000,
                "conservative_maximum_tier": True,
                "matching_dimension_count": 1,
                "selected_dimension": {
                    "description": "primary billing view API request",
                    "effective_date": authority_date.isoformat(),
                    "pricing_scope": "PUBLISHED_FIXED_RATE",
                    "rate_id": "aws-cost-explorer-primary-billing-view-request",
                    "source": COST_EXPLORER_PRICE_URL,
                    "unit": "Request",
                },
            }
        )
    for service_code in PRICE_SERVICES:
        dimensions: list[dict[str, Any]] = []
        token: str | None = None
        tokens: set[str] = set()
        while True:
            kwargs: dict[str, Any] = {
                "ServiceCode": service_code,
                "Filters": [{"Type": "TERM_MATCH", "Field": "regionCode", "Value": REGION}],
                "FormatVersion": "aws_v1",
                "MaxResults": 100,
            }
            if token:
                kwargs["NextToken"] = token
            response = pricing.get_products(**kwargs)
            for encoded in response.get("PriceList", []):
                dimensions.extend(
                    ondemand_dimensions(
                        json.loads(encoded), region=REGION, observed_at_epoch=observed_at_epoch
                    )
                )
            token = response.get("NextToken")
            if not token:
                break
            if token in tokens or len(tokens) >= 512:
                raise LiveEvidenceError("pricing pagination cycle or bounded page limit exceeded")
            tokens.add(token)
        for line in profile["lines"]:
            if line["service_code"] != service_code:
                continue
            matches = [
                row
                for row in dimensions
                if row["unit"] in line["units"]
                and re.search(
                    line["usage_pattern"],
                    str(row["attributes"].get("usagetype", "")) + " " + row["description"],
                    re.IGNORECASE,
                )
            ]
            if not matches and line["component"] == "dashboard":
                for usage in ("DashboardsUsageHour", "DashboardsUsageHour-Basic"):
                    global_response = pricing.get_products(
                        ServiceCode="AmazonCloudWatch",
                        Filters=[
                            {"Type": "TERM_MATCH", "Field": "location", "Value": "Any"},
                            {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage},
                        ],
                        FormatVersion="aws_v1",
                        MaxResults=100,
                    )
                    if global_response.get("NextToken"):
                        raise LiveEvidenceError("exact global dashboard catalog is incomplete")
                    for encoded in global_response.get("PriceList", []):
                        rows = ondemand_dimensions(
                            json.loads(encoded),
                            region=REGION,
                            observed_at_epoch=observed_at_epoch,
                            allow_global_dashboard=True,
                        )
                        matches.extend(
                            row
                            for row in rows
                            if row["pricing_scope"] == "ACCOUNT_GLOBAL_DASHBOARD"
                            and row["unit"] == "Dashboards"
                            and row["unit"] in line["units"]
                        )
            if not matches:
                global_dashboard_shapes: list[dict[str, Any]] = []
                if line["component"] == "dashboard":
                    # Diagnose account-global catalog entries explicitly. They are
                    # not admitted as regional prices by this observation.
                    global_response = pricing.get_products(
                        ServiceCode="AmazonCloudWatch",
                        Filters=[{"Type": "TERM_MATCH", "Field": "location", "Value": "Any"}],
                        FormatVersion="aws_v1",
                        MaxResults=100,
                    )
                    for encoded in global_response.get("PriceList", []):
                        product = json.loads(encoded)
                        attributes = product.get("product", {}).get("attributes", {})
                        if "dashboard" not in str(attributes.get("usagetype", "")).casefold():
                            continue
                        for term in product.get("terms", {}).get("OnDemand", {}).values():
                            for dimension in term.get("priceDimensions", {}).values():
                                global_dashboard_shapes.append(
                                    {
                                        "location": attributes.get("location"),
                                        "region_code": attributes.get("regionCode"),
                                        "usage": attributes.get("usagetype"),
                                        "unit": dimension.get("unit"),
                                        "usd": dimension.get("pricePerUnit", {}).get("USD"),
                                        "description": dimension.get("description"),
                                    }
                                )
                catalog_shapes = sorted(
                    {
                        (str(row["unit"]), str(row["attributes"].get("usagetype", "")))
                        for row in dimensions
                    }
                )
                diagnostic_shapes = sorted(
                    catalog_shapes,
                    key=lambda shape: (
                        not bool(re.search(line["usage_pattern"], shape[1], re.IGNORECASE)),
                        shape[0].casefold().rstrip("s")
                        not in {unit.casefold().rstrip("s") for unit in line["units"]},
                        shape,
                    ),
                )
                missing_rates.append(
                    f"no current exact-region OnDemand USD rate for {line['component']}; "
                    f"observed units: {sorted({shape[0] for shape in catalog_shapes})}; "
                    f"observed unit/usage shapes (first 32 of {len(catalog_shapes)}): "
                    f"{diagnostic_shapes[:32]}"
                    f"; unadmitted global dashboard observations: {global_dashboard_shapes[:8]}"
                )
                continue
            # Charge all units at the maximum applicable tier, with no free-tier deduction.
            selected = max(matches, key=lambda row: (row["unit_cost_microusd"], row["rate_id"]))
            observations.append(
                {
                    "component": line["component"],
                    "service_code": service_code,
                    "region": REGION,
                    "unit_cost_microusd": selected["unit_cost_microusd"],
                    "conservative_maximum_tier": True,
                    "matching_dimension_count": len(matches),
                    "selected_dimension": {
                        key: value for key, value in selected.items() if key != "attributes"
                    },
                }
            )
    if missing_rates:
        raise LiveEvidenceError(" | ".join(missing_rates))
    return observations


def _cost_envelope(
    observed_at_epoch: int, profile: Mapping[str, Any], rates: list[dict[str, Any]]
) -> dict[str, Any]:
    expected_false_claims = (
        "managed_runtime_executed",
        "teardown_executed",
        "billed_cost_observed",
    )
    if (
        profile.get("contract") != COST_PROFILE_CONTRACT
        or profile.get("region") != REGION
        or profile.get("planning_bounds_verified") is not True
        or profile.get("verification_scope") != COST_PROFILE_SCOPE
        or profile.get("cost_horizon_days") != 30
        or profile.get("maximum_workflow_executions") != 1
        or profile.get("planned_max_input_rows") != 1_000
        or profile.get("planned_max_output_rows") != 5_000
        or profile.get("maximum_explicit_object_bytes") != 32 * 1024 * 1024
        or profile.get("maximum_explicit_run_objects") != 9
        or any(profile.get(field) is not False for field in expected_false_claims)
    ):
        raise LiveEvidenceError("Stage 6 planning-bound authority is incomplete or overstated")
    quantities = {
        line.get("component"): line.get("quantity_millionths")
        for line in profile.get("lines", [])
        if isinstance(line, Mapping)
    }
    if len(quantities) != len(profile.get("lines", [])) or quantities != EXPECTED_COST_QUANTITIES:
        raise LiveEvidenceError("Stage 6 cost quantities differ from the reviewed planning bound")
    kms_lines = [
        line
        for line in profile.get("lines", [])
        if isinstance(line, Mapping) and line.get("component") == "kms-requests"
    ]
    if len(kms_lines) != 1 or any(
        kms_lines[0].get(field) != value for field, value in EXPECTED_KMS_REQUEST_SELECTOR.items()
    ):
        raise LiveEvidenceError(
            "Stage 6 KMS price selector differs from symmetric request authority"
        )
    compute = Path("infra/terraform/compute.tf").read_text(encoding="utf-8")
    main = Path("infra/terraform/main.tf").read_text(encoding="utf-8")
    launch = Path("src/featureforge/glue_launch.py").read_text(encoding="utf-8")
    immutable = Path("src/featureforge/s3_immutable.py").read_text(encoding="utf-8")
    required_compute = (
        'worker_type            = "G.1X"',
        "number_of_workers      = 2",
        "timeout                = 15",
        "max_retries            = 0",
        "memory_size                    = 512",
        "timeout                        = 300",
        "reserved_concurrent_executions = 1",
        "MaxAttempts     = 3",
        "TimeoutSeconds = 3600",
    )
    if (
        any(token not in compute for token in required_compute)
        or compute.count("states:::lambda:invoke") != 7
        or '"--TempDir"' in compute
        or "MAX_GLUE_LAUNCHES = 3" not in launch
        or "MAX_OBJECT_BYTES = 32 * 1024 * 1024" not in immutable
        or main.count("force_destroy = true") != 3
        or 'customer_master_key_spec = "SYMMETRIC_DEFAULT"' not in main
        or 'key_usage                = "ENCRYPT_DECRYPT"' not in main
        or 'id     = "stage6-thirty-day-cost-horizon"' not in main
        or "noncurrent_days = 30" not in main
        or "days_after_initiation = 1" not in main
    ):
        raise LiveEvidenceError("Stage 6 cost profile differs from executable source controls")
    fixed = [
        line
        for line in profile.get("lines", [])
        if line.get("service_code") == "DOCUMENTED_FIXED_RATE"
    ]
    if len(fixed) != 1 or fixed[0].get("component") != "cost-explorer-requests":
        raise LiveEvidenceError("Cost Explorer request charge is missing from the frozen worksheet")
    return priced_cost_envelope(profile, rates, observed_at_epoch=observed_at_epoch)


def _financial_visibility(
    account_id: str, observed_at_epoch: int, worst_case_microusd: int
) -> dict[str, Any]:
    client = _client("budgets", "us-east-1")
    rows: list[dict[str, Any]] = []
    token: str | None = None
    seen: set[str] = set()
    while True:
        kwargs: dict[str, Any] = {
            "AccountId": account_id,
            "MaxResults": 100,
            "ShowFilterExpression": True,
        }
        if token:
            kwargs["NextToken"] = token
        page = client.describe_budgets(**kwargs)
        rows.extend(page.get("Budgets", []))
        token = page.get("NextToken")
        if not token:
            break
        if token in seen or len(seen) >= 100:
            raise LiveEvidenceError("budget pagination is cyclic or excessive")
        seen.add(token)
    headroom = budget_headroom(
        rows, observed_at_epoch=observed_at_epoch, worst_case_microusd=worst_case_microusd
    )
    today = datetime.fromtimestamp(observed_at_epoch, UTC).date()
    start = today.replace(day=1).isoformat()
    end = (today + timedelta(days=1)).isoformat()
    cost = _client("ce", "us-east-1").get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
    )
    if cost.get("NextPageToken"):
        raise LiveEvidenceError("Cost Explorer pagination exceeds the one-request authority")
    if not cost.get("ResultsByTime"):
        raise LiveEvidenceError("Cost Explorer returned no current-month visibility")
    return {
        "account_cost_visibility_verified": True,
        "account_spend_not_used_to_reduce_project_ceiling": True,
        "budget_authority": headroom,
    }


def qualify(run_id: str, source_branch: str) -> dict[str, Any]:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise LiveEvidenceError("run ID must be a 3-16 character lowercase identifier")
    observed_at = int(time.time())
    identity = _client("sts").get_caller_identity()
    account_id = str(identity["Account"])
    if fingerprint(account_id) != EXPECTED_ACCOUNT_FINGERPRINT:
        raise LiveEvidenceError("qualification identity is outside the authorized account")
    sanitized = sanitized_identity(account_id, str(identity["Arn"]), ROLE_NAME)
    backend, _ = _verify_backend(account_id)
    inventory = _inventory(account_id, run_id)
    profile = json.loads(Path("docs/stage6/cost-workload-profile.json").read_text())
    prices = _pricing(observed_at, profile)
    cost = _cost_envelope(observed_at, profile, prices)
    receipt: dict[str, Any] = {
        "account": sanitized,
        "backend": backend,
        "bootstrap_writes_excluded_from_read_only_window": [
            "OIDC_ROLE_CREATE_AND_TRUST_UPDATE",
            "BACKEND_BUCKET_SECURITY_CONFIGURATION",
            "INITIAL_EMPTY_STATE_CONDITIONAL_CREATE",
        ],
        "cost_envelope": cost,
        "cost_profile_sha256": _sha(canonical_json(profile).encode()),
        "financial_visibility": _financial_visibility(
            account_id, observed_at, cost["worst_case_microusd"]
        ),
        "inventory": {
            "digest": _sha(canonical_json(inventory).encode("utf-8")),
            "empty": True,
            "items": inventory,
        },
        "managed_workload_executed": False,
        "observed_at_epoch": observed_at,
        "observed_at_utc": datetime.fromtimestamp(observed_at, UTC).isoformat(),
        "oidc": _verify_oidc(account_id, source_branch),
        "pricing": prices,
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
