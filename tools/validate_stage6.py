#!/usr/bin/env python3
"""Fail-closed local validator for Part 3 Stage 1 / global Stage 6."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from featureforge.canonical import canonical_json, digest

try:
    from tools.build_stage6_artifacts import build
except ModuleNotFoundError:  # direct `python tools/validate_stage6.py` execution
    from build_stage6_artifacts import build

PROJECT = "featureforge-ml-feature-platform"
BASE = "86b3cd27ae95a6142a1d6601d188d83b8e783d29"
BASE_TREE = "a9aeb81af407af5fb2108e9d2ef3a194770b9761"
ACCEPTANCE = {f"ST6-AC-{number:02d}" for number in range(1, 25)}
INDEXED = (
    "contracts/stage6-candidate-write-projection-v1.json",
    "tests/test_stage6_capacity_preflight.py",
    "contracts/stage6-parity-summary-v1.json",
    "contracts/stage6-frozen-expected-v1.json",
    "src/featureforge/s3_immutable.py",
    "contracts/stage6-glue-launch-v1.json",
    "contracts/stage6-glue-output-authority-v2.json",
    "src/featureforge/glue_launch.py",
    "tests/test_stage6_glue_launch.py",
    "contracts/stage6-admission-authority-v1.json",
    "src/featureforge/managed_admission.py",
    "tests/test_stage6_managed_admission.py",
    ".github/workflows/ci.yml",
    ".github/workflows/aws-oidc-identity.yml",
    ".github/workflows/stage6-aws-qualification.yml",
    ".github/workflows/terraform.yml",
    ".gitignore",
    "Makefile",
    "README.md",
    "contracts/stage6-completion-receipt-v1.json",
    "contracts/stage6-cost-envelope-v1.json",
    "contracts/stage6-lease-snapshot-v1.json",
    "contracts/stage6-managed-run-v1.json",
    "contracts/stage6-plan-authority-v1.json",
    "contracts/stage6-residual-inventory-v1.json",
    "contracts/stage6-task-v1.json",
    "docs/architecture.md",
    "docs/claims.yaml",
    "docs/runbook.md",
    "docs/stage6/aws-read-api-manifest.json",
    "docs/stage6/autonomous-recovery.md",
    "docs/stage6/autonomy-checkpoint.md",
    "docs/stage6/claims.json",
    "docs/stage6/cleanup-matrix.md",
    "docs/stage6/cost-model.md",
    "docs/stage6/cost-workload-profile.json",
    "docs/stage6/failure-repair-log.md",
    "docs/stage6/iam-matrix.md",
    "docs/stage6/lease-procedure.md",
    "docs/stage6/managed-architecture.md",
    "docs/stage6/plan-authority.md",
    "docs/stage6/predecessor.json",
    "docs/stage6/requirements.json",
    "docs/stage6/traceability.json",
    "docs/stage6/walkthrough.md",
    "evidence/stage6/artifact-qualification.json",
    "evidence/stage6/baseline.json",
    "evidence/stage6/bootstrap-receipt.json",
    "evidence/stage6/failure-recovery-proof.json",
    "evidence/stage6/local-proof.json",
    "evidence/stage6/plan-retry-failure-observation.json",
    "evidence/stage6/plan-retry-prior-observation.json",
    "evidence/stage6/recovery-observation.json",
    "evidence/stage6/saved-retry-finalization-receipt.json",
    "evidence/stage6/historical-reconstitution/normalized-plan.json",
    "evidence/stage6/historical-reconstitution/plan-authority.json",
    "evidence/stage6/historical-reconstitution/no-mutation-receipt.json",
    "evidence/stage6/historical-reconstitution/reconstitution-report.json",
    "evidence/stage6/toolchain-qualification.json",
    "infra/terraform/.terraform.lock.hcl",
    "infra/terraform/README.md",
    "infra/terraform/compute.tf",
    "infra/terraform/iam.tf",
    "infra/terraform/main.tf",
    "infra/terraform/observability.tf",
    "infra/terraform/outputs.tf",
    "infra/terraform/variables.tf",
    "infra/terraform/versions.tf",
    "jobs/glue_point_in_time.py",
    "pyproject.toml",
    "src/featureforge/aws_runtime.py",
    "src/featureforge/control_worker.py",
    "src/featureforge/managed.py",
    "src/featureforge/stage6_live.py",
    "tests/stage6_oracle.py",
    "tests/test_stage6_contract.py",
    "tests/test_stage6_managed.py",
    "tests/test_stage6_live.py",
    "tests/test_stage6_cost_authority.py",
    "tests/test_stage6_glue_provenance.py",
    "tests/test_stage6_infra_security.py",
    "tests/test_stage6_oracle_authority.py",
    "tests/test_stage6_orchestration_shape.py",
    "tests/test_stage6_plan_execution.py",
    "tests/test_stage6_saved_plan_finalization.py",
    "tests/test_stage6_reconstitution.py",
    "tests/test_stage6_recovery.py",
    "tests/test_stage6_runtime_effects.py",
    "tests/test_stage6_strict_manifest.py",
    "tests/test_stage6_terraform.py",
    "tools/build_stage6_artifacts.py",
    "tools/execute_stage6_plan.py",
    "tools/finalize_stage6_saved_plan.py",
    "tools/run_stage6_proof.py",
    "tools/qualify_stage6_aws.py",
    "tools/recover_stage6_readonly.py",
    "tools/reconstitute_stage6_historical.py",
    "tools/validate_stage6.py",
)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Stage 6 validation failed: {message}")


def _load(root: Path, name: str) -> dict[str, Any]:
    value: dict[str, Any] = json.loads((root / name).read_text(encoding="utf-8"))
    return value


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_index(root: Path) -> dict[str, Any]:
    return {
        "authorized_base_commit": BASE,
        "authorized_base_tree": BASE_TREE,
        "files_sha256": {name: _sha(root / name) for name in INDEXED},
        "project": PROJECT,
        "stage": 6,
    }


def _verify_embedded_digest(value: dict[str, Any], field: str) -> str:
    body = dict(value)
    recorded = body.pop(field, None)
    _check(isinstance(recorded, str), f"{field} missing")
    _check(recorded == digest(body), f"{field} does not bind content")
    return recorded


def _validate_oracle(root: Path) -> None:
    tree = ast.parse((root / "tests/stage6_oracle.py").read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    _check(
        not any(name == "featureforge" or name.startswith("featureforge.") for name in imports),
        "independent oracle imports production FeatureForge code",
    )


def _validate_contracts(root: Path) -> None:
    paths = sorted((root / "contracts").glob("stage6-*.json"))
    _check(len(paths) == 13, "exactly thirteen Stage 6 contracts are required")
    for path in paths:
        value = json.loads(path.read_text(encoding="utf-8"))
        _check(str(value.get("$id", "")).startswith("urn:featureforge:stage6-"), path.name)
        _check(value.get("additionalProperties") is False, f"open contract: {path.name}")
        _check(bool(value.get("required")), f"unbounded contract: {path.name}")


def _validate_proofs(root: Path) -> None:
    local = _load(root, "evidence/stage6/local-proof.json")
    recovery = _load(root, "evidence/stage6/failure-recovery-proof.json")
    _verify_embedded_digest(local, "proof_digest")
    _verify_embedded_digest(recovery, "proof_digest")
    _check(local.get("project") == PROJECT, "local proof project mismatch")
    _check(recovery.get("project") == PROJECT, "recovery proof project mismatch")
    _check(local.get("checks") and all(local["checks"].values()), "local proof check failed")
    _check(
        recovery.get("checks") and all(recovery["checks"].values()),
        "failure/recovery proof check failed",
    )
    _check(
        len(recovery.get("admission_controls", [])) == 6,
        "admission negative-control coverage drift",
    )
    _check(
        len(recovery.get("stale_plan_controls", [])) == 14,
        "saved-plan negative-control coverage drift",
    )


def _validate_artifacts(root: Path) -> None:
    evidence = _load(root, "evidence/stage6/artifact-qualification.json")
    with tempfile.TemporaryDirectory(prefix="featureforge-stage6-artifacts-") as temporary:
        manifest = build(Path(temporary))
    expected = [
        {
            "maximum_bytes": row["maximum_bytes"],
            "name": row["name"],
            "sha256": row["sha256"],
            "size_bytes": row["size_bytes"],
        }
        for row in manifest["artifacts"]
    ]
    _check(evidence.get("artifacts") == expected, "artifact evidence differs from clean build")
    _check(
        evidence.get("artifact_manifest_sha256")
        == hashlib.sha256((canonical_json(manifest) + "\n").encode()).hexdigest(),
        "artifact manifest digest mismatch",
    )
    _check(evidence.get("byte_identical_rebuild") is True, "reproducible-build gate open")
    for field in (
        "allowlist_only",
        "tests_included",
        "caches_included",
        "credentials_included",
        "terraform_state_included",
        "local_paths_included",
        "binaries_committed",
    ):
        expected_value = field == "allowlist_only"
        _check(evidence.get(field) is expected_value, f"artifact boundary failed: {field}")


def _validate_terraform(root: Path) -> None:
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((root / "infra/terraform").glob("*.tf"))
    )
    compute = (root / "infra/terraform/compute.tf").read_text(encoding="utf-8")
    versions = (root / "infra/terraform/versions.tf").read_text(encoding="utf-8")
    lock = (root / "infra/terraform/.terraform.lock.hcl").read_text(encoding="utf-8")
    workflow = (root / ".github/workflows/terraform.yml").read_text(encoding="utf-8")
    _check("states:::aws-sdk:glue:getJobRun" in compute, "Glue completion integration missing")
    _check('action                 = "START_GLUE"' in compute, "budgeted Glue launch missing")
    _check(
        "self.glue.start_job_run(**counted_request)"
        in (root / "src/featureforge/glue_launch.py").read_text(),
        "physical Glue launch missing",
    )
    _check(compute.count("states:::lambda:invoke") >= 5, "Lambda integrations incomplete")
    _check('Type = "Pass"' not in compute and 'Type  = "Pass"' not in compute, "Pass state")
    _check('state               = "DISABLED"' in compute, "schedule is not disabled")
    _check("reserved_concurrent_executions = 1" in compute, "Lambda concurrency drift")
    _check('required_version = "= 1.9.8"' in versions, "Terraform pin drift")
    _check('version = "= 5.100.0"' in versions, "provider pin drift")
    _check('version     = "5.100.0"' in lock, "provider lock drift")
    _check(
        terraform.count("force_destroy = true") == 3,
        "every exact Stage 6 bucket must be covered by the reviewed Terraform destroy inverse",
    )
    _check(
        'resource "aws_s3_bucket_lifecycle_configuration" "managed"' in terraform
        and 'id     = "stage6-thirty-day-cost-horizon"' in terraform
        and "noncurrent_days = 30" in terraform,
        "versioned object cost horizon is not configured",
    )
    _check('"--TempDir"' not in compute, "unbounded Glue S3 temporary prefix is configured")
    _check("runtime_execution_enabled == false" in terraform, "plan-only runtime gate missing")
    _check(
        "terraform plan" not in workflow and "terraform apply" not in workflow,
        "CI mutates or plans",
    )
    _check("aws-actions/configure-aws-credentials" not in workflow, "CI obtains AWS credentials")
    _check(
        "actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683" in workflow,
        "checkout action is not pinned",
    )
    _check(
        "hashicorp/setup-terraform@b9cd54a3c349d3f38e8881555d616ced269862dd" in workflow,
        "Terraform action is not pinned",
    )
    qualification = (root / ".github/workflows/stage6-aws-qualification.yml").read_text(
        encoding="utf-8"
    )
    _check(
        "aws-actions/configure-aws-credentials@e6de054238d6b7531b4efff3b6587d9aade6a06c"
        in qualification,
        "AWS credentials action is not pinned",
    )


def _validate_read_manifest(root: Path) -> None:
    manifest = _load(root, "docs/stage6/aws-read-api-manifest.json")
    actions = manifest.get("actions", [])
    _check(manifest.get("mutating_actions") == [], "AWS manifest records mutations")
    _check("sts:GetCallerIdentity" in actions, "identity qualification missing")
    _check(
        "s3:GetEncryptionConfiguration" in actions,
        "S3 bucket-encryption IAM action missing",
    )
    _check(
        "s3:GetBucketEncryption" not in actions,
        "S3 API operation name incorrectly used as an IAM action",
    )
    mutation = re.compile(r":(?:Put|Create|Delete|Update|Start|Invoke|Stop|Tag|Untag)")
    _check(not any(mutation.search(str(action)) for action in actions), "mutating AWS API allowed")


def _validate_plan_executor(root: Path) -> None:
    executor = (root / "tools/execute_stage6_plan.py").read_text(encoding="utf-8")
    finalizer = (root / "tools/finalize_stage6_saved_plan.py").read_text(encoding="utf-8")
    recovery = (root / "tools/recover_stage6_readonly.py").read_text(encoding="utf-8")
    live = (root / "src/featureforge/stage6_live.py").read_text(encoding="utf-8")
    retry_observation = _load(root, "evidence/stage6/plan-retry-prior-observation.json")
    failure_observation = _load(
        root, "evidence/stage6/plan-retry-failure-observation.json"
    )
    _verify_embedded_digest(retry_observation, "receipt_sha256")
    _check(
        retry_observation.get("contract") == "stage6-plan-retry-prior-observation-v1"
        and retry_observation.get("aws_writes_executed") is False
        and retry_observation.get("lease_expired_at_observation") is True
        and retry_observation.get("delete_marker_count") == 0,
        "retry prior observation is not read-only, expired and marker-free",
    )
    _check(
        _sha(root / "evidence/stage6/plan-retry-failure-observation.json")
        == "5124d08f63f384222ee3564b0b0db521fe5b4c4f92f4c66962843b3db2f44366"
        and failure_observation.get("aws_writes_executed_by_observation") is False
        and failure_observation.get("source_commit")
        == "2acd63e2222ada623d277dbd4f5a8032dea8e376"
        and failure_observation.get("lease", {}).get("version_count") == 3
        and failure_observation.get("saved_plan", {}).get("resource_count") == 47,
        "retry failure observation is not exact, read-only and plan-bound",
    )
    _check(executor.count("s3.put_object(**request)") == 1, "lease writer count differs")
    _check('"IfMatch": previous_etag' in executor, "successor lease is not CAS guarded")
    _check('"IfNoneMatch"' not in executor, "successor executor uses initial-acquisition guard")
    _check('"ChecksumAlgorithm": "SHA256"' in executor, "lease checksum request missing")
    _check('"ServerSideEncryption": "AES256"' in executor, "lease encryption request missing")
    _check('"total_max_attempts": 1' in executor, "plan executor SDK can physically retry")
    _check('"TF_DATA_DIR"' in executor, "Terraform data is not privately isolated")
    _check('"-refresh=true"' in executor, "Terraform plan is not refresh aware")
    _check('"-lock=false"' in executor, "Terraform plan can mutate the state lock")
    for command in ('"apply"', '"destroy"', '"import"'):
        _check(command not in executor, f"forbidden Terraform command implemented: {command}")
    _check(
        executor.count("qualifier._verify_backend(account)") == 3,
        "backend state is not checked before and after the successor plan",
    )
    _check(
        executor.count("qualifier._inventory(account, RUN_ID)") == 3,
        "residual inventory is not checked before and after the successor plan",
    )
    _check(
        "LEASE_VERSION_FINGERPRINT" in recovery
        and "_version_with_fingerprint" in recovery
        and '"lease_successor_versions_present"' in recovery,
        "historical recovery does not preserve the pinned lease across successors",
    )
    _check(
        '"aws_s3_bucket_lifecycle_configuration"' in live
        and '"s3_lifecycle_cost_horizon"' in live
        and '"stage6-thirty-day-cost-horizon"' in live,
        "saved-plan normalization does not strictly validate the lifecycle cost horizon",
    )
    _check(
        'f"/aws-glue/jobs/{prefix}/"' in live,
        "saved-plan normalization omits the exact managed Glue log namespace",
    )
    for forbidden in (
        ".put_object(",
        ".delete_object(",
        '"apply"',
        '"destroy"',
        '"import"',
        '[str(terraform), "-chdir=infra/terraform", "plan"',
    ):
        _check(
            forbidden not in finalizer,
            f"saved-plan finalizer contains forbidden path: {forbidden}",
        )
    _check(
        '"init"' in finalizer
        and '"-backend=false"' in finalizer
        and '"validate"' in finalizer
        and '"show"' in finalizer
        and '"current_execution_authority": False' in finalizer,
        "saved-plan finalizer does not preserve its read-only historical boundary",
    )


def _validate_bootstrap_receipt(root: Path) -> None:
    receipt = _load(root, "evidence/stage6/bootstrap-receipt.json")
    _check(receipt.get("project") == PROJECT, "bootstrap project mismatch")
    _check(receipt.get("stage") == 6, "bootstrap stage mismatch")
    _check(receipt.get("region") == "ap-southeast-2", "bootstrap region mismatch")
    for field in (
        "account_fingerprint",
        "role_arn_fingerprint",
        "oidc_provider_fingerprint",
        "backend_bucket_fingerprint",
        "backend_key_fingerprint",
        "state_lineage_fingerprint",
        "state_sha256",
        "state_version_fingerprint",
    ):
        value = receipt.get(field)
        _check(
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value),
            f"invalid bootstrap fingerprint: {field}",
        )
    _check(receipt.get("immutable_owner_id") == "276895096", "owner ID drift")
    _check(receipt.get("immutable_repository_id") == "1332971230", "repository ID drift")
    _check(receipt.get("oidc_audience") == "sts.amazonaws.com", "OIDC audience drift")
    for field in (
        "oidc_identity_verified",
        "backend_region_verified",
        "backend_versioning_enabled",
        "backend_public_access_blocked",
        "backend_owner_enforced",
        "backend_tls_only",
        "state_conditional_create",
        "state_retrieval_byte_identical",
    ):
        _check(receipt.get(field) is True, f"bootstrap control failed: {field}")
    _check(receipt.get("backend_public") is False, "backend is public")
    _check(receipt.get("backend_default_encryption") == "AES256", "backend encryption drift")
    _check(receipt.get("state_contract_version") == 4, "state version drift")
    _check(receipt.get("state_terraform_version") == "1.9.8", "state tool version drift")
    _check(receipt.get("state_serial") == 0, "initial state serial drift")
    _check(receipt.get("state_resources") == 0, "initial state is not empty")
    _check(receipt.get("terraform_apply_executed") is False, "Terraform apply was executed")
    _check(
        receipt.get("runtime_resource_mutation_executed") is False,
        "runtime resource mutation was executed",
    )
    _check(receipt.get("managed_workload_executed") is False, "managed workload was executed")
    _check(
        receipt.get("authorized_bootstrap_writes")
        == [
            "OIDC_ROLE_CREATE_AND_TRUST_UPDATE",
            "BACKEND_BUCKET_SECURITY_CONFIGURATION",
            "INITIAL_EMPTY_STATE_CONDITIONAL_CREATE",
        ],
        "bootstrap write inventory drift",
    )


def validate(root: Path, expect_head: str | None = None) -> None:
    requirements = _load(root, "docs/stage6/requirements.json")
    traceability = _load(root, "docs/stage6/traceability.json")
    _check(
        {row["id"] for row in requirements.get("acceptance_criteria", [])} == ACCEPTANCE,
        "requirements must contain exactly ST6-AC-01 through ST6-AC-24",
    )
    _check(
        {row["acceptance"] for row in traceability.get("mappings", [])} == ACCEPTANCE,
        "traceability must map every and only Stage 6 criterion",
    )
    predecessor = _load(root, "docs/stage6/predecessor.json")
    baseline = _load(root, "evidence/stage6/baseline.json")
    for value in (predecessor, baseline):
        _check(value.get("authorized_base_commit") == BASE, "authorized base mismatch")
        _check(value.get("authorized_base_tree") == BASE_TREE, "authorized tree mismatch")
    _check(predecessor.get("predecessor_status") == "verified-complete", "predecessor open")
    _check(baseline.get("predecessor_proofs_byte_identical") is True, "predecessor proofs drift")
    for field in (
        "aws_read_only_executed",
        "aws_runtime_executed",
        "terraform_plan_executed",
        "terraform_applied",
        "terraform_destroyed",
        "managed_workload_executed",
    ):
        _check(baseline.get(field) is False, f"unsupported execution claim: {field}")
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    _check("validate_stage6.py" in workflow, "CI does not run Stage 6 validation")
    _check("run_stage6_proof" in workflow, "CI does not reproduce Stage 6 proof")
    _check("build_stage6_artifacts" in workflow, "CI does not rebuild Stage 6 artifacts")
    _validate_contracts(root)
    _validate_oracle(root)
    _validate_proofs(root)
    _validate_artifacts(root)
    _validate_terraform(root)
    _validate_read_manifest(root)
    _validate_plan_executor(root)
    _validate_bootstrap_receipt(root)
    finalization = _load(root, "evidence/stage6/saved-retry-finalization-receipt.json")
    _check(
        finalization.get("contract") == "stage6-saved-retry-finalization-receipt-v1",
        "saved retry finalization contract drift",
    )
    _check(finalization.get("resource_count") == 47, "saved retry resource count drift")
    _check(
        finalization.get("plan_action_counts") == {"create": 47},
        "saved retry action count drift",
    )
    _check(
        finalization.get("lifecycle_resource_count") == 3,
        "saved retry lifecycle count drift",
    )
    _check(finalization.get("aws_write_count") == 0, "finalizer performed an AWS write")
    _check(
        finalization.get("new_terraform_plan_count") == 0,
        "finalizer executed a new Terraform plan",
    )
    _check(
        finalization.get("current_execution_authority") is False,
        "historical saved plan is presented as current authority",
    )
    for field in (
        "terraform_apply_executed",
        "terraform_destroy_executed",
        "terraform_import_executed",
    ):
        _check(finalization.get(field) is False, f"unsupported finalization claim: {field}")
    _check(finalization.get("state_bytes_unchanged") is True, "state changed during finalization")
    _check(
        finalization.get("terraform_show_reproduced_saved_json") is True,
        "saved plan JSON was not reproduced",
    )
    claims = _load(root, "docs/stage6/claims.json")
    _check(
        {row.get("class") for row in claims.get("claims", [])}
        == {"LOCAL_VERIFIED", "PENDING", "STATICALLY_VALIDATED", "UNCLAIMED"},
        "Stage 6 claim classes are incomplete",
    )
    contamination_files = (
        "docs/stage6/managed-architecture.md",
        "docs/stage6/walkthrough.md",
        "evidence/stage6/failure-recovery-proof.json",
        "evidence/stage6/local-proof.json",
        "jobs/glue_point_in_time.py",
        "src/featureforge/aws_runtime.py",
        "src/featureforge/control_worker.py",
        "src/featureforge/managed.py",
    )
    rendered = "\n".join(
        (root / name).read_text(encoding="utf-8").lower() for name in contamination_files
    )
    _check(
        not any(
            project in rendered
            for project in ("ledgerguard", "changebridge", "eventpulse", "atlas retail")
        ),
        "other-project contamination",
    )
    if (root / ".git").exists():
        tracked = subprocess.run(
            ["git", "ls-files"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.splitlines()
        generated = [
            name
            for name in tracked
            if Path(name).suffix in {".sqlite", ".db", ".pyc", ".tfplan"}
            or Path(name).name == ".coverage"
            or Path(name).parts[:1] == ("build",)
        ]
        _check(not generated, "generated runtime or private plan artifact is tracked")
    _check(
        _load(root, "evidence/stage6/evidence-index.json") == _build_index(root),
        "Stage 6 evidence index is stale or incomplete",
    )
    if expect_head is not None:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip()
        _check(head == expect_head, "repository HEAD differs from expected exact head")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write-index", action="store_true")
    parser.add_argument("--expect-head")
    args = parser.parse_args()
    if args.write_index:
        target = args.root / "evidence/stage6/evidence-index.json"
        target.write_text(canonical_json(_build_index(args.root)) + "\n", encoding="utf-8")
        print(f"wrote {target}")
        return
    validate(args.root, args.expect_head)
    print(
        "Stage 6 local structural and deterministic checks passed. This is not an acceptance "
        "closure receipt: managed integration, current AWS/cost/lease/plan authority, "
        "exact-head governance, and continuation gates require separate evidence."
    )


if __name__ == "__main__":
    main()
