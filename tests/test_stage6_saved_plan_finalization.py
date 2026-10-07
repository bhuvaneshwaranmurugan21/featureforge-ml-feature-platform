"""Pure boundary tests for the read-only saved-plan finalizer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import finalize_stage6_saved_plan as finalizer


def successor_lease() -> dict[str, object]:
    return {
        "contract": "stage6-lease-snapshot-v1",
        "lease_id": "c" * 32,
        "owner": finalizer.RUN_ID,
        "source_commit": finalizer.SOURCE_COMMIT,
        "acquired_at_epoch": 1_000,
        "heartbeat_at_epoch": 1_000,
        "expires_at_epoch": 1_000 + 3_300,
    }


def test_historical_plan_window_requires_both_original_authorities() -> None:
    assert (
        finalizer.historical_plan_window(
            qualification_completed_at=900,
            plan_created_at=1_100,
            lease_acquired_at=1_000,
            lease_expires_at=4_300,
        )
        == 4_300
    )
    with pytest.raises(finalizer.SavedPlanFinalizationError, match="qualification"):
        finalizer.historical_plan_window(
            qualification_completed_at=900,
            plan_created_at=4_500,
            lease_acquired_at=1_000,
            lease_expires_at=5_000,
        )
    with pytest.raises(finalizer.SavedPlanFinalizationError, match="lease"):
        finalizer.historical_plan_window(
            qualification_completed_at=900,
            plan_created_at=950,
            lease_acquired_at=1_000,
            lease_expires_at=4_300,
        )


def test_successor_lease_is_exact_source_owner_lifetime_and_plan_current() -> None:
    proof = finalizer.successor_lease_proof(successor_lease(), plan_created_at=1_100)
    assert proof["source_commit"] == finalizer.SOURCE_COMMIT
    assert proof["owner"] == finalizer.RUN_ID
    changed = successor_lease() | {"expires_at_epoch": 4_301}
    with pytest.raises(finalizer.SavedPlanFinalizationError, match="lifetime"):
        finalizer.successor_lease_proof(changed, plan_created_at=1_100)
    with pytest.raises(Exception, match="not current"):
        finalizer.successor_lease_proof(successor_lease(), plan_created_at=4_300)


def test_saved_plan_identifiers_are_bound_to_observed_private_evidence() -> None:
    assert finalizer.PLAN_TIMESTAMP == "2026-10-06T08:55:53Z"
    assert finalizer.BINARY_PLAN_SHA256 == (
        "9f1d0ee441aff40e37bf0a3091bf1f0448f3974d012e0d58c3013d04227dc7ec"
    )
    assert finalizer.RAW_PLAN_SHA256 == (
        "c27bd50dc9dbabd05b238ca661177ff2e566441682e0112f56afd13b8b6b4126"
    )
    assert finalizer.SUCCESSOR_LEASE_OBJECT_SHA256 == (
        "8fd398f278015ab5d5339849c8d1f34712e0d2b236b7616474d934f04c291342"
    )
    assert finalizer.SUCCESSOR_LEASE_VERSION_FINGERPRINT == (
        "cfc0db7925addc22d85dbdc11af2460d211058cfb666f786412a4d3a77bae8b1"
    )
    assert finalizer.LEASE_VERSION_COUNT == 3


def test_failure_observation_is_exact_and_semantically_bound() -> None:
    value = finalizer._validate_failure_observation()
    assert value["source_commit"] == finalizer.SOURCE_COMMIT
    assert value["source_tree"] == finalizer.SOURCE_TREE
    assert value["lease"]["version_count"] == 3
    assert value["saved_plan"]["resource_count"] == 47
    assert value["aws_writes_executed_by_observation"] is False


def test_finalizer_has_no_aws_write_or_new_terraform_plan_path() -> None:
    source = Path(finalizer.__file__).read_text(encoding="utf-8")
    for forbidden in (
        ".put_object(",
        ".delete_object(",
        ".create_bucket(",
        ".create_role(",
        ".create_function(",
        ".start_job_run(",
        '"apply"',
        '"destroy"',
        '"import"',
        '[str(terraform), "-chdir=infra/terraform", "plan"',
    ):
        assert forbidden not in source
    assert '"validate"' in source
    assert '"show"' in source
    assert '"init"' in source
    assert '"-backend=false"' in source


def test_published_finalization_receipt_is_historical_and_zero_mutation() -> None:
    receipt = json.loads(
        (
            finalizer.ROOT
            / "evidence/stage6/saved-retry-finalization-receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["source_commit"] == finalizer.SOURCE_COMMIT
    assert receipt["source_tree"] == finalizer.SOURCE_TREE
    assert receipt["resource_count"] == 47
    assert receipt["plan_action_counts"] == {"create": 47}
    assert receipt["lifecycle_resource_count"] == 3
    assert receipt["lease_version_count"] == 3
    assert receipt["aws_write_count"] == 0
    assert receipt["new_terraform_plan_count"] == 0
    assert receipt["state_bytes_unchanged"] is True
    assert receipt["current_execution_authority"] is False
    assert receipt["terraform_apply_executed"] is False
    assert receipt["terraform_destroy_executed"] is False
    assert receipt["terraform_import_executed"] is False
