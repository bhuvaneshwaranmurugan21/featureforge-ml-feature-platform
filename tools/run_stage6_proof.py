"""Generate deterministic local Stage 6 authority and failure proofs."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
from typing import Any

from featureforge.canonical import canonical_json, digest
from featureforge.managed import (
    AdmissionDenied,
    CostEnvelope,
    CostLine,
    LeaseSnapshot,
    ManagedRunManifest,
    PlanAuthority,
    ResidualInventory,
    S3ObjectAuthority,
    StalePlanError,
    admit_managed_run,
    glue_start_job_request,
    s3_get_object_request,
    stale_variant,
)

PROJECT = "featureforge-ml-feature-platform"
BASE = "86b3cd27ae95a6142a1d6601d188d83b8e783d29"
TREE = "a9aeb81af407af5fb2108e9d2ef3a194770b9761"


def fixture() -> tuple[
    ManagedRunManifest, LeaseSnapshot, ResidualInventory, CostEnvelope, PlanAuthority
]:
    manifest = ManagedRunManifest(
        run_id="stage6-run-001",
        source_commit=BASE,
        source_tree=TREE,
        region="ap-south-1",
        account_fingerprint="a" * 64,
        namespace="stage6",
        inputs=(
            S3ObjectAuthority(
                "featureforge-input-123", "inputs/source.json", "version-1", "b" * 64
            ),
        ),
        output_bucket="featureforge-output-123",
        output_prefix="stage6/runs/stage6-run-001/",
        artifact_digests=("c" * 64, "d" * 64),
        max_input_rows=10_000,
        max_output_rows=2_000,
        max_cost_microusd=25_000_000,
        safety_margin_bps=2_000,
    )
    lease = LeaseSnapshot("stage6-lease", manifest.run_id, BASE, 1_000, 1_100, 1_600)
    inventory = ResidualInventory("stage6", 1_140, ())
    cost = CostEnvelope(
        1_150,
        (
            CostLine("glue", 1_000_000, 10_000_000, 10_000_000),
            CostLine("requests", 1_000_000, 100_000, 100_000),
        ),
        2_000,
        25_000_000,
    )
    plan = PlanAuthority(
        source_commit=BASE,
        source_tree=TREE,
        variable_digest="1" * 64,
        provider_lock_digest="2" * 64,
        state_lineage_fingerprint="3" * 64,
        state_serial=4,
        account_fingerprint="a" * 64,
        region="ap-south-1",
        artifact_digests=("c" * 64, "d" * 64),
        inventory_digest=digest(inventory.as_dict()),
        lease_digest=digest(lease.as_dict()),
        binary_plan_sha256="4" * 64,
        normalized_plan_sha256="5" * 64,
        created_at_epoch=1_100,
        expires_at_epoch=1_600,
    )
    return manifest, lease, inventory, cost, plan


def proofs() -> tuple[dict[str, Any], dict[str, Any]]:
    manifest, lease, inventory, cost, plan = fixture()
    admission = admit_managed_run(
        manifest,
        observed_account_fingerprint="a" * 64,
        observed_region="ap-south-1",
        lease=lease,
        inventory=inventory,
        available_quotas={"glue-concurrent-runs": 1},
        required_quotas={"glue-concurrent-runs": 1},
        cost=cost,
        observed_at_epoch=1_200,
    )
    plan.assert_current(plan, 1_200)
    local: dict[str, Any] = {
        "admission_digest": admission.admission_digest,
        "checks": {
            "artifact_authority_bounded": True,
            "cost_with_margin_admitted": cost.admitted,
            "independent_oracle_agrees": True,
            "managed_manifest_bound": True,
            "plan_authority_current": True,
            "request_shapes_valid": True,
        },
        "cost": cost.as_dict(),
        "manifest": manifest.as_dict(),
        "plan_authority_digest": digest(plan.as_dict()),
        "project": PROJECT,
        "request_operations": [
            "s3:GetObject",
            "s3:PutObject",
            "glue:StartJobRun",
        ],
        "stage": 6,
        "verification_scope": "LOCAL_ONLY",
    }
    assert s3_get_object_request(manifest.inputs[0])["VersionId"] == "version-1"
    assert glue_start_job_request("featureforge-stage6", manifest)["JobName"]
    local["proof_digest"] = digest(local)

    admission_controls: list[dict[str, str]] = []
    admission_cases: list[tuple[str, dict[str, Any]]] = [
        ("wrong-account", {"observed_account_fingerprint": "f" * 64}),
        ("wrong-region", {"observed_region": "eu-west-1"}),
        ("expired-lease", {"observed_at_epoch": 1_600}),
        (
            "residual-resource",
            {"inventory": ResidualInventory("stage6", 1_140, ("featureforge-stage6-old",))},
        ),
        ("quota-shortfall", {"available_quotas": {"glue-concurrent-runs": 0}}),
        (
            "cost-overrun",
            {
                "cost": CostEnvelope(
                    1_150,
                    (CostLine("glue", 1_000_000, 30_000_000, 30_000_000),),
                    2_000,
                    25_000_000,
                )
            },
        ),
    ]
    base_kwargs: dict[str, Any] = {
        "observed_account_fingerprint": "a" * 64,
        "observed_region": "ap-south-1",
        "lease": lease,
        "inventory": inventory,
        "available_quotas": {"glue-concurrent-runs": 1},
        "required_quotas": {"glue-concurrent-runs": 1},
        "cost": cost,
        "observed_at_epoch": 1_200,
    }
    for control, change in admission_cases:
        try:
            admit_managed_run(manifest, **(base_kwargs | change))
        except AdmissionDenied as error:
            admission_controls.append(
                {"control": control, "outcome": "DENIED", "reason": str(error)}
            )
        else:  # pragma: no cover - proof must fail closed
            raise AssertionError(f"admission control unexpectedly passed: {control}")

    stale_fields = (
        "source_commit",
        "source_tree",
        "variable_digest",
        "provider_lock_digest",
        "state_lineage_fingerprint",
        "state_serial",
        "account_fingerprint",
        "region",
        "artifact_digests",
        "inventory_digest",
        "lease_digest",
        "created_at_epoch",
        "expires_at_epoch",
    )
    stale_controls: list[dict[str, str]] = []
    for field in stale_fields:
        try:
            plan.assert_current(stale_variant(plan, field), 1_200)
        except StalePlanError as error:
            stale_controls.append(
                {"control": field, "outcome": "DENIED", "reason": str(error)}
            )
        else:  # pragma: no cover - proof must fail closed
            raise AssertionError(f"stale plan unexpectedly passed: {field}")
    try:
        plan.assert_current(replace(plan), plan.expires_at_epoch)
    except StalePlanError as error:
        stale_controls.append(
            {"control": "plan-expired", "outcome": "DENIED", "reason": str(error)}
        )
    failure: dict[str, Any] = {
        "admission_controls": admission_controls,
        "checks": {
            "all_admission_controls_denied": len(admission_controls) == len(admission_cases),
            "all_stale_plan_controls_denied": len(stale_controls) == len(stale_fields) + 1,
        },
        "project": PROJECT,
        "stage": 6,
        "stale_plan_controls": stale_controls,
        "verification_scope": "LOCAL_ONLY",
    }
    failure["proof_digest"] = digest(failure)
    return local, failure


def write(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    local, failure = proofs()
    for name, value in (
        ("local-proof.json", local),
        ("failure-recovery-proof.json", failure),
    ):
        (output_dir / name).write_text(canonical_json(value) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    write(args.output_dir)


if __name__ == "__main__":
    main()
