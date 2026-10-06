"""Pure boundary tests for the read-only saved-plan finalizer."""

from __future__ import annotations

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
    assert finalizer.PLAN_TIMESTAMP == "2026-10-06T05:37:10Z"
    assert finalizer.BINARY_PLAN_SHA256 == (
        "21283629c85ce06bad6166f505bbbd0d750505ea293802e326f16d85f2747620"
    )
    assert finalizer.RAW_PLAN_SHA256 == (
        "cf3df22c415c2d8c17045bdf15f6e979f82fe72427c0b9735870c3cd28c0ca7e"
    )
    assert finalizer.SUCCESSOR_LEASE_OBJECT_SHA256 == (
        "f425d87569905859407f9e50b93762321ea86ff3b5476cc53631574b4b9718c4"
    )


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
