"""Pure validation and sanitization for Stage 6 live AWS-plan evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, cast

from featureforge.canonical import canonical_json, digest
from featureforge.managed import LeaseSnapshot, ManagedContractError

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
