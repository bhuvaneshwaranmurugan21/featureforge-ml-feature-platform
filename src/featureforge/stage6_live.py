"""Pure validation and sanitization for Stage 6 live AWS-plan evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any, cast

from featureforge.canonical import canonical_json, digest
from featureforge.managed import CostEnvelope, CostLine, LeaseSnapshot, ManagedContractError

ALLOWED_ACTIONS = frozenset({("create",), ("no-op",), ("read",)})
ALLOWED_RESOURCE_TYPES = frozenset(
    {
        "aws_cloudwatch_dashboard",
        "aws_cloudwatch_event_rule",
        "aws_cloudwatch_event_target",
        "aws_cloudwatch_log_group",
        "aws_cloudwatch_metric_alarm",
        "aws_dynamodb_table",
        "aws_glue_catalog_database",
        "aws_glue_job",
        "aws_glue_security_configuration",
        "aws_iam_role",
        "aws_iam_role_policy",
        "aws_kms_alias",
        "aws_kms_key",
        "aws_lambda_function",
        "aws_s3_bucket",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_versioning",
        "aws_s3_object",
        "aws_sfn_state_machine",
    }
)


class LiveEvidenceError(ValueError):
    """Live evidence is missing, stale, unsafe, or contains an unexpected action."""


def usd_microusd(value: Any) -> int:
    """Convert USD conservatively, rejecting nonfinite or negative money."""
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise ValueError("invalid USD amount")
        return int((amount * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise LiveEvidenceError("USD amount must be finite and nonnegative") from error


def ondemand_dimensions(
    product: Mapping[str, Any],
    *,
    region: str,
    observed_at_epoch: int,
    allow_global_dashboard: bool = False,
) -> list[dict[str, Any]]:
    """Extract real OnDemand USD dimensions, never a catalog-sample surrogate."""
    identity = product.get("product", {})
    attributes = identity.get("attributes", {})
    global_dashboard = (
        allow_global_dashboard
        and attributes.get("servicecode") == "AmazonCloudWatch"
        and attributes.get("location") == "Any"
        and attributes.get("regionCode") == ""
        and attributes.get("usagetype") in {"DashboardsUsageHour", "DashboardsUsageHour-Basic"}
    )
    if attributes.get("regionCode") != region and not global_dashboard:
        raise LiveEvidenceError("pricing product is not in the authorized region")
    sku = identity.get("sku")
    publication = product.get("publicationDate")
    if not isinstance(sku, str) or not sku or not isinstance(publication, str):
        raise LiveEvidenceError("pricing SKU or publication provenance is absent")
    rows = []
    for term_id, term in product.get("terms", {}).get("OnDemand", {}).items():
        effective = term.get("effectiveDate")
        try:
            effective_epoch = int(
                datetime.fromisoformat(effective.replace("Z", "+00:00")).timestamp()
            )
        except (AttributeError, ValueError, TypeError) as error:
            raise LiveEvidenceError("pricing effective-date provenance is invalid") from error
        if effective_epoch > observed_at_epoch:
            continue
        for rate_id, dimension in term.get("priceDimensions", {}).items():
            if "USD" not in dimension.get("pricePerUnit", {}):
                raise LiveEvidenceError("pricing dimension is missing USD")
            rows.append(
                {
                    "sku": sku,
                    "term_id": term_id,
                    "rate_id": rate_id,
                    "effective_at_epoch": effective_epoch,
                    "publication_date": publication,
                    "unit": dimension.get("unit"),
                    "begin_range": dimension.get("beginRange"),
                    "end_range": dimension.get("endRange"),
                    "unit_cost_microusd": usd_microusd(dimension["pricePerUnit"]["USD"]),
                    "attributes": dict(attributes),
                    "description": str(dimension.get("description", "")),
                    "catalog_sha256": sha256_bytes(canonical_json(product).encode()),
                    "pricing_scope": "ACCOUNT_GLOBAL_DASHBOARD" if global_dashboard else "REGIONAL",
                }
            )
    return rows


def priced_cost_envelope(
    profile: Mapping[str, Any], rates: Sequence[Mapping[str, Any]], *, observed_at_epoch: int
) -> dict[str, Any]:
    expected = {line["component"] for line in profile["lines"]}
    actual = [str(rate["component"]) for rate in rates]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise LiveEvidenceError("pricing coverage must match every frozen component exactly")
    indexed = {str(rate["component"]): rate for rate in rates}
    lines = []
    for row in profile["lines"]:
        quantity = row["quantity_millionths"]
        rate = indexed[row["component"]]["unit_cost_microusd"]
        if type(quantity) is not int or type(rate) is not int:
            raise LiveEvidenceError("price and quantity must be integer fixed-point values")
        lines.append(
            CostLine(row["component"], quantity, rate, (quantity * rate + 999_999) // 1_000_000)
        )
    result = CostEnvelope(observed_at_epoch, tuple(lines), 2_000, 25_000_000)
    if not result.admitted:
        raise LiveEvidenceError(
            "priced cost envelope exceeds USD 25 including twenty-percent margin"
        )
    return result.as_dict()


def budget_headroom(
    budgets: Sequence[Mapping[str, Any]], *, observed_at_epoch: int, worst_case_microusd: int
) -> dict[str, Any]:
    """Select an applicable account-wide gross USD monthly cost budget, not its mere existence."""
    applicable = []
    for budget in budgets:
        period = budget.get("TimePeriod", {})
        start, end = period.get("Start"), period.get("End")
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        cost_types = budget.get("CostTypes", {})
        if (
            budget.get("BudgetType") != "COST"
            or budget.get("TimeUnit") != "MONTHLY"
            or budget.get("BudgetLimit", {}).get("Unit") != "USD"
            or budget.get("CostFilters")
            or budget.get("FilterExpression")
            or cost_types.get("IncludeCredit") is not False
            or cost_types.get("IncludeRefund") is not False
            or not (
                start.replace(tzinfo=start.tzinfo or UTC).timestamp()
                <= observed_at_epoch
                < end.replace(tzinfo=end.tzinfo or UTC).timestamp()
            )
        ):
            continue
        spent = budget.get("CalculatedSpend", {})
        actual, forecast = spent.get("ActualSpend", {}), spent.get("ForecastedSpend", {})
        if actual.get("Unit") != "USD" or forecast.get("Unit") != "USD":
            continue
        usd_microusd(budget["BudgetLimit"]["Amount"])
        limit = int(
            (Decimal(str(budget["BudgetLimit"]["Amount"])) * 1_000_000).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        observed = max(usd_microusd(actual["Amount"]), usd_microusd(forecast["Amount"]))
        applicable.append((limit - observed, budget, limit, observed))
    if not applicable:
        raise LiveEvidenceError(
            "no applicable current gross-USD monthly account budget with actual and forecast"
        )
    # Multiple applicable account budgets are all constraints: use the most restrictive.
    headroom, selected, limit, spent = min(applicable, key=lambda row: row[0])
    if headroom < worst_case_microusd:
        raise LiveEvidenceError(
            "actual/forecast budget headroom is below the complete cost envelope"
        )
    return {
        "applicable_budget_count": len(applicable),
        "budget_fingerprint": fingerprint(str(selected["BudgetName"])),
        "budget_limit_microusd": limit,
        "actual_or_forecast_microusd": spent,
        "available_headroom_microusd": headroom,
        "required_microusd": worst_case_microusd,
        "credits_and_refunds_excluded": True,
        "headroom_verified": True,
    }


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def fingerprint(value: str) -> str:
    if not value:
        raise LiveEvidenceError("cannot fingerprint an empty identity")
    return sha256_bytes(value.encode("utf-8"))


def validate_initial_state(
    state_bytes: bytes,
    *,
    expected_lineage_fingerprint: str,
    expected_sha256: str,
) -> dict[str, Any]:
    if sha256_bytes(state_bytes) != expected_sha256:
        raise LiveEvidenceError("backend state checksum differs from authorized bootstrap")
    try:
        state = json.loads(state_bytes)
    except json.JSONDecodeError as error:
        raise LiveEvidenceError("backend state is not JSON") from error
    expected = {
        "version": 4,
        "terraform_version": "1.9.8",
        "serial": 0,
        "outputs": {},
        "resources": [],
        "check_results": None,
    }
    for field, value in expected.items():
        if state.get(field) != value:
            raise LiveEvidenceError(f"backend state field drift: {field}")
    lineage = state.get("lineage")
    if not isinstance(lineage, str) or fingerprint(lineage) != expected_lineage_fingerprint:
        raise LiveEvidenceError("backend state lineage differs from authorized bootstrap")
    return {
        "lineage_fingerprint": expected_lineage_fingerprint,
        "serial": 0,
        "sha256": expected_sha256,
        "terraform_version": "1.9.8",
    }


def validate_lease(
    value: Mapping[str, Any],
    *,
    source_commit: str,
    owner: str,
    observed_at_epoch: int,
) -> dict[str, Any]:
    try:
        lease = LeaseSnapshot(
            lease_id=str(value["lease_id"]),
            owner=str(value["owner"]),
            source_commit=str(value["source_commit"]),
            acquired_at_epoch=int(value["acquired_at_epoch"]),
            heartbeat_at_epoch=int(value["heartbeat_at_epoch"]),
            expires_at_epoch=int(value["expires_at_epoch"]),
        )
    except (KeyError, TypeError, ValueError, ManagedContractError) as error:
        raise LiveEvidenceError("exclusive lease contract is invalid") from error
    if value.get("contract") != "stage6-lease-snapshot-v1":
        raise LiveEvidenceError("exclusive lease contract name is invalid")
    if lease.source_commit != source_commit:
        raise LiveEvidenceError("exclusive lease is bound to another commit")
    if lease.owner != owner:
        raise LiveEvidenceError("exclusive lease is owned by another run")
    if not (lease.heartbeat_at_epoch <= observed_at_epoch < lease.expires_at_epoch):
        raise LiveEvidenceError("exclusive lease is not current")
    return {
        "acquired_at_epoch": lease.acquired_at_epoch,
        "digest": digest(lease.as_dict()),
        "expires_at_epoch": lease.expires_at_epoch,
        "heartbeat_at_epoch": lease.heartbeat_at_epoch,
        "lease_id_fingerprint": fingerprint(lease.lease_id),
        "owner": lease.owner,
        "source_commit": lease.source_commit,
    }


def _resource_changes(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = plan.get("resource_changes")
    if not isinstance(raw, list) or not raw:
        raise LiveEvidenceError("Terraform plan has no resource changes")
    rows: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise LiveEvidenceError("Terraform resource change is not an object")
        address = item.get("address")
        resource_type = item.get("type")
        change = item.get("change")
        if not isinstance(address, str) or not isinstance(resource_type, str):
            raise LiveEvidenceError("Terraform resource identity is invalid")
        if item.get("mode") == "data":
            continue
        if resource_type not in ALLOWED_RESOURCE_TYPES:
            raise LiveEvidenceError(f"Terraform resource type is not allowlisted: {resource_type}")
        if not isinstance(change, Mapping) or not isinstance(change.get("actions"), list):
            raise LiveEvidenceError(f"Terraform change actions are missing: {address}")
        actions = tuple(str(action) for action in change["actions"])
        if actions not in ALLOWED_ACTIONS:
            raise LiveEvidenceError(f"Terraform action is not allowlisted: {address} {actions}")
        if any(action in {"delete", "update"} for action in actions):
            raise LiveEvidenceError(f"Terraform destructive or mutable action rejected: {address}")
        rows.append(
            {
                "actions": list(actions),
                "address": address,
                "name": str(item.get("name", "")),
                "type": resource_type,
            }
        )
    return sorted(rows, key=lambda row: row["address"])


def _find_after(plan: Mapping[str, Any], address: str) -> Mapping[str, Any]:
    for item in plan.get("resource_changes", []):
        if isinstance(item, Mapping) and item.get("address") == address:
            change = item.get("change")
            if isinstance(change, Mapping) and isinstance(change.get("after"), Mapping):
                return cast(Mapping[str, Any], change["after"])
    raise LiveEvidenceError(f"required Terraform resource is absent: {address}")


def normalize_terraform_plan(plan: Mapping[str, Any], *, run_id: str) -> dict[str, Any]:
    rows = _resource_changes(plan)
    prefix = f"featureforge-stage6-{run_id}"
    for row in rows:
        after = _find_after(plan, row["address"])
        for key in ("name", "function_name", "alarm_name", "dashboard_name"):
            value = after.get(key)
            if isinstance(value, str) and value and not _name_in_namespace(value, prefix):
                raise LiveEvidenceError(
                    f"Terraform resource escapes FeatureForge namespace: {row['address']}"
                )

    event = _find_after(plan, "aws_cloudwatch_event_rule.materialization")
    worker = _find_after(plan, "aws_lambda_function.control_worker")
    glue = _find_after(plan, "aws_glue_job.offline")
    control = _find_after(plan, "aws_dynamodb_table.control")
    online = _find_after(plan, "aws_dynamodb_table.online")
    checks = {
        "all_actions_allowlisted": True,
        "event_schedule_disabled": event.get("state") == "DISABLED",
        "glue_concurrency_one": _nested_equals(
            glue.get("execution_property"), "max_concurrent_runs", 1
        ),
        "glue_timeout_fifteen_minutes": glue.get("timeout") == 15,
        "glue_two_g1x_workers": glue.get("number_of_workers") == 2
        and glue.get("worker_type") == "G.1X",
        "lambda_concurrency_one": worker.get("reserved_concurrent_executions") == 1,
        "control_table_pitr": _nested_enabled(control.get("point_in_time_recovery")),
        "online_table_pitr": _nested_enabled(online.get("point_in_time_recovery")),
        "online_table_ttl": _nested_enabled(online.get("ttl")),
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise LiveEvidenceError(f"Terraform plan invariant failed: {', '.join(failed)}")
    action_counts: dict[str, int] = {}
    for row in rows:
        key = "+".join(row["actions"])
        action_counts[key] = action_counts.get(key, 0) + 1
    normalized = {
        "action_counts": dict(sorted(action_counts.items())),
        "checks": checks,
        "format_version": str(plan.get("format_version", "")),
        "resource_changes": rows,
        "resource_count": len(rows),
        "terraform_version": str(plan.get("terraform_version", "")),
    }
    normalized["normalized_plan_sha256"] = sha256_bytes(
        (canonical_json(normalized) + "\n").encode("utf-8")
    )
    return normalized


def _nested_enabled(value: Any) -> bool:
    return _nested_equals(value, "enabled", True)


def _nested_equals(value: Any, key: str, expected: Any) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        return False
    if len(value) != 1:
        return False
    first = value[0]
    return isinstance(first, Mapping) and first.get(key) == expected


def _name_in_namespace(value: str, prefix: str) -> bool:
    return (
        value.startswith(prefix)
        or value.startswith(f"alias/{prefix}")
        or value.startswith(f"/aws/lambda/{prefix}")
        or value.startswith(f"/aws/vendedlogs/states/{prefix}")
        or value.startswith(prefix.replace("-", "_"))
    )


def sanitized_identity(account_id: str, caller_arn: str, role_name: str) -> dict[str, Any]:
    expected_fragment = f":assumed-role/{role_name}/"
    if expected_fragment not in caller_arn:
        raise LiveEvidenceError("AWS caller is not the authorized FeatureForge role")
    return {
        "account_fingerprint": fingerprint(account_id),
        "caller_arn_fingerprint": fingerprint(caller_arn),
        "role_name": role_name,
        "verified": True,
    }
