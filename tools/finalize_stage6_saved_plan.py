#!/usr/bin/env python3
"""Finalize the already-created Stage 6 plan without another lease write or plan.

This recovery command is intentionally read-only with respect to AWS and Terraform
planning. It authenticates the exact saved binary/raw plan pair, the exact source
worktree, the single successor lease version, the unchanged serial-zero state and
the still-empty inventory. It may run ``terraform validate`` and ``terraform show``;
it has no plan, apply, destroy, import, or AWS write path.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
import time
import zipfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from botocore.exceptions import BotoCoreError, ClientError

from featureforge.canonical import canonical_json, digest
from featureforge.managed import PlanAuthority
from featureforge.stage6_live import (
    LiveEvidenceError,
    fingerprint,
    normalize_terraform_plan,
    sha256_bytes,
    validate_lease,
)
from tools import execute_stage6_plan as executor
from tools import qualify_stage6_aws as qualifier

ROOT = Path(__file__).resolve().parents[1]
SOURCE_COMMIT = "8fc29d36f39a8cb8105f004b594bbedea8292d77"
SOURCE_TREE = "41814b95c2e5dde78e9cf0d3b62a7ad7a9b455a2"
ACCOUNT = "857229544428"
REGION = "ap-southeast-2"
RUN_ID = "s6-plan-20260930"
LEASE_KEY = "leases/featureforge/stage6.json"
STATE_KEY = "state/stage6/terraform.tfstate"
LEASE_CAP = 16_384
QUALIFICATION_RUN_ID = 37_416_197_533
QUALIFICATION_RECEIPT_SHA256 = (
    "a74c42fdbf7fab362e34f29b43d524ea2383c2e71562374efb05bbc7e3ae6199"
)
PLAN_TIMESTAMP = "2026-10-06T05:37:10Z"
BINARY_PLAN_SHA256 = "21283629c85ce06bad6166f505bbbd0d750505ea293802e326f16d85f2747620"
RAW_PLAN_SHA256 = "cf3df22c415c2d8c17045bdf15f6e979f82fe72427c0b9735870c3cd28c0ca7e"
SUCCESSOR_LEASE_OBJECT_SHA256 = (
    "f425d87569905859407f9e50b93762321ea86ff3b5476cc53631574b4b9718c4"
)
CONFIRMATION = "FINALIZE_EXISTING_PLAN_READ_ONLY"


class SavedPlanFinalizationError(RuntimeError):
    """The saved plan cannot be authenticated without repeating an operation."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SavedPlanFinalizationError(message)


def _sha_file(path: Path) -> str:
    return cast(str, sha256_bytes(path.read_bytes()))


def _parse_timestamp(value: str) -> int:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SavedPlanFinalizationError("saved plan timestamp is invalid") from error
    _require(parsed.tzinfo is not None, "saved plan timestamp has no timezone")
    return int(parsed.astimezone(UTC).timestamp())


def historical_plan_window(
    *,
    qualification_completed_at: int,
    plan_created_at: int,
    lease_acquired_at: int,
    lease_expires_at: int,
) -> int:
    """Prove the saved plan was created inside both original authority windows."""
    for value in (
        qualification_completed_at,
        plan_created_at,
        lease_acquired_at,
        lease_expires_at,
    ):
        _require(type(value) is int and value > 0, "historical authority epoch is invalid")
    _require(
        qualification_completed_at <= plan_created_at < qualification_completed_at + 3_600,
        "saved plan was outside the exact-head qualification window",
    )
    _require(
        lease_acquired_at <= plan_created_at < lease_expires_at,
        "saved plan was outside the successor lease window",
    )
    expiry = min(qualification_completed_at + 3_600, lease_expires_at, plan_created_at + 3_600)
    _require(plan_created_at < expiry, "saved plan historical authority interval is empty")
    return expiry


def successor_lease_proof(value: Mapping[str, Any], *, plan_created_at: int) -> dict[str, Any]:
    expected_fields = {
        "contract",
        "lease_id",
        "owner",
        "source_commit",
        "acquired_at_epoch",
        "heartbeat_at_epoch",
        "expires_at_epoch",
    }
    _require(set(value) == expected_fields, "successor lease fields differ")
    for field in ("lease_id", "owner", "source_commit", "contract"):
        _require(isinstance(value.get(field), str), f"successor lease {field} is invalid")
    for field in ("acquired_at_epoch", "heartbeat_at_epoch", "expires_at_epoch"):
        _require(type(value.get(field)) is int, f"successor lease {field} is invalid")
    acquired = value["acquired_at_epoch"]
    _require(value["heartbeat_at_epoch"] == acquired, "successor lease heartbeat drifted")
    _require(
        value["expires_at_epoch"] - acquired == executor.LEASE_SECONDS,
        "lease lifetime differs",
    )
    return cast(
        dict[str, Any],
        validate_lease(
            value,
            source_commit=SOURCE_COMMIT,
            owner=RUN_ID,
            observed_at_epoch=plan_created_at,
        ),
    )


def _git(repository: Path, *args: str) -> str:
    environment = dict(os.environ)
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    result = subprocess.run(
        ["git", "-C", str(repository), *args],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _require_clean_exact_source(source_root: Path) -> None:
    _require(source_root.is_dir(), "source worktree is absent")
    _require(_git(source_root, "rev-parse", "HEAD") == SOURCE_COMMIT, "source commit differs")
    _require(
        _git(source_root, "rev-parse", "HEAD^{tree}") == SOURCE_TREE,
        "source tree differs",
    )
    _require(not _git(source_root, "diff", "--name-only"), "source worktree has unstaged changes")
    _require(
        not _git(source_root, "diff", "--cached", "--name-only"),
        "source worktree has staged changes",
    )
    _require(
        not _git(source_root, "ls-files", "--others", "--exclude-standard"),
        "source worktree has untracked files",
    )


def _run(
    command: Sequence[str],
    *,
    cwd: Path,
    environment_overrides: Mapping[str, str] | None = None,
) -> tuple[bytes, bytes]:
    environment = dict(os.environ)
    environment.update(
        {
            "AWS_EC2_METADATA_DISABLED": "true",
            "CHECKPOINT_DISABLE": "1",
            "TF_IN_AUTOMATION": "1",
        }
    )
    if environment_overrides:
        environment.update(environment_overrides)
    result = subprocess.run(
        list(command), cwd=cwd, env=environment, check=False, capture_output=True
    )
    _require(result.returncode == 0, f"read-only command failed: {command[0]} {command[1]}")
    return result.stdout, result.stderr


def _write_exclusive(path: Path, value: str | bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(descriptor, "wb" if isinstance(value, bytes) else "w") as handle:
        handle.write(value)


def _write_public(path: Path, value: Mapping[str, Any]) -> None:
    _write_exclusive(path, canonical_json(value) + "\n")


def _public_archive(public_dir: Path, output: Path) -> None:
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(public_dir.iterdir()):
            info = zipfile.ZipInfo(path.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            info.create_system = 3
            archive.writestr(info, path.read_bytes(), compresslevel=9)


def _read_version(s3: Any, bucket: str, version_id: str) -> bytes:
    request = {
        "Bucket": bucket,
        "ExpectedBucketOwner": ACCOUNT,
        "Key": LEASE_KEY,
        "VersionId": version_id,
    }
    head = s3.head_object(**request)
    response = s3.get_object(**request)
    stream = response.get("Body")
    try:
        for metadata in (head, response):
            _require(metadata.get("VersionId") == version_id, "lease version read differs")
            _require(metadata.get("ServerSideEncryption") == "AES256", "lease is not AES256")
            length = metadata.get("ContentLength")
            _require(type(length) is int and 0 < length <= LEASE_CAP, "lease size is invalid")
        _require(head["ContentLength"] == response["ContentLength"], "lease length drifted")
        _require(stream is not None, "lease body is absent")
        body = stream.read(LEASE_CAP + 1)
        _require(isinstance(body, bytes), "lease body is not bytes")
        _require(len(body) == response["ContentLength"], "lease body is truncated")
        return cast(bytes, body)
    finally:
        if stream is not None:
            stream.close()


def _decode_object(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SavedPlanFinalizationError(f"{label} is not valid JSON") from error
    _require(isinstance(value, dict), f"{label} is not an object")
    return cast(dict[str, Any], value)


def _validate_private_inputs(failed_output: Path, source_root: Path) -> tuple[Path, Path, Path]:
    private = failed_output / "private"
    public = failed_output / "public"
    _require(private.is_dir() and public.is_dir(), "failed executor layout is incomplete")
    _require(not any(public.iterdir()), "failed executor unexpectedly emitted public evidence")
    binary_plan = private / "stage6.tfplan"
    raw_plan = private / "stage6-plan.json"
    variables = private / "stage6.auto.tfvars.json"
    backend = private / "backend.hcl"
    for path in (binary_plan, raw_plan, variables, backend):
        _require(
            path.is_file() and not path.is_symlink(),
            f"saved private file is absent: {path.name}",
        )
    _require(_sha_file(binary_plan) == BINARY_PLAN_SHA256, "binary plan digest differs")
    _require(_sha_file(raw_plan) == RAW_PLAN_SHA256, "raw plan digest differs")
    expected_backend = (
        f'bucket = "featureforge-stage6-tfstate-{ACCOUNT}-{REGION}"\n'
        f'key = "{STATE_KEY}"\nregion = "{REGION}"\nencrypt = true\n'
    )
    _require(backend.read_text(encoding="utf-8") == expected_backend, "backend input differs")
    value = _decode_object(variables.read_bytes(), "saved variable file")
    expected_keys = {
        "admission_authority",
        "aws_region",
        "control_worker_zip_path",
        "github_oidc_provider_arn",
        "glue_library_zip_path",
        "glue_script_path",
        "run_id",
        "runtime_execution_enabled",
    }
    _require(set(value) == expected_keys, "saved variable fields differ")
    _require(value["admission_authority"] is None, "saved plan contains runtime authority")
    _require(value["aws_region"] == REGION and value["run_id"] == RUN_ID, "saved scope differs")
    _require(value["runtime_execution_enabled"] is False, "saved runtime gate is enabled")
    _require(
        value["github_oidc_provider_arn"]
        == f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com",
        "saved OIDC provider differs",
    )
    expected_paths = {
        "control_worker_zip_path": private / "build-a/featureforge-control-worker.zip",
        "glue_library_zip_path": private / "build-a/featureforge-glue-library.zip",
        "glue_script_path": source_root / "jobs/glue_point_in_time.py",
    }
    for field, expected in expected_paths.items():
        _require(Path(value[field]).resolve() == expected.resolve(), f"saved path differs: {field}")
        _require(expected.is_file(), f"saved input is absent: {field}")
    return binary_plan, raw_plan, variables


def _validate_artifacts(source_root: Path, failed_output: Path, private: Path) -> tuple[str, ...]:
    build_a = private / "rebuild-a"
    build_b = private / "rebuild-b"
    build_a.mkdir(mode=0o700)
    build_b.mkdir(mode=0o700)
    environment = {
        "PYTHONPATH": f"{source_root / 'src'}:{source_root}",
    }
    command = [sys.executable, "-m", "tools.build_stage6_artifacts"]
    _run(
        [*command, "--output-dir", str(build_a)],
        cwd=source_root,
        environment_overrides=environment,
    )
    _run(
        [*command, "--output-dir", str(build_b)],
        cwd=source_root,
        environment_overrides=environment,
    )
    names = ("featureforge-control-worker.zip", "featureforge-glue-library.zip")
    qualification = _decode_object(
        (source_root / "evidence/stage6/artifact-qualification.json").read_bytes(),
        "source artifact qualification",
    )
    expected = {row["name"]: row["sha256"] for row in qualification.get("artifacts", [])}
    _require(set(expected) == set(names), "source artifact qualification coverage differs")
    digests = []
    for name in names:
        first = (build_a / name).read_bytes()
        _require(first == (build_b / name).read_bytes(), f"artifact rebuild differs: {name}")
        _require(
            first == (failed_output / "private/build-a" / name).read_bytes(),
            f"saved plan artifact differs: {name}",
        )
        checksum = sha256_bytes(first)
        _require(checksum == expected[name], f"artifact evidence differs: {name}")
        digests.append(checksum)
    digests.append(_sha_file(source_root / "jobs/glue_point_in_time.py"))
    return tuple(sorted(digests))


def _validate_terraform_saved_plan(
    terraform: Path,
    source_root: Path,
    failed_output: Path,
    binary_plan: Path,
    raw_plan: Path,
    private: Path,
) -> dict[str, Any]:
    _require(terraform.is_file() and os.access(terraform, os.X_OK), "Terraform binary is absent")
    version_out, version_err = _run([str(terraform), "version", "-json"], cwd=source_root)
    _write_exclusive(private / "terraform-version.err", version_err)
    version = _decode_object(version_out, "Terraform version output")
    _require(version.get("terraform_version") == "1.9.8", "Terraform version differs")
    terraform_data = failed_output / "private/terraform-data"
    _require(terraform_data.is_dir(), "saved Terraform data directory is absent")
    environment = {"TF_DATA_DIR": str(terraform_data)}
    validate_out, validate_err = _run(
        [str(terraform), "-chdir=infra/terraform", "validate", "-no-color"],
        cwd=source_root,
        environment_overrides=environment,
    )
    _write_exclusive(private / "terraform-validate.log", validate_out + validate_err)
    shown, show_err = _run(
        [str(terraform), "-chdir=infra/terraform", "show", "-json", str(binary_plan)],
        cwd=source_root,
        environment_overrides=environment,
    )
    _write_exclusive(private / "terraform-show.err", show_err)
    _require(shown == raw_plan.read_bytes(), "binary plan does not reproduce saved raw JSON")
    value = _decode_object(shown, "saved Terraform plan")
    _require(value.get("timestamp") == PLAN_TIMESTAMP, "saved Terraform timestamp differs")
    _require(value.get("terraform_version") == "1.9.8", "saved plan Terraform version differs")
    normalized = cast(dict[str, Any], normalize_terraform_plan(value, run_id=RUN_ID))
    _require(normalized.get("resource_count") == 47, "saved managed resource count differs")
    _require(normalized.get("action_counts") == {"create": 47}, "saved plan actions differ")
    return normalized


def _live_read_only_proofs(plan_created_at: int) -> dict[str, Any]:
    identity = executor._client("sts").get_caller_identity()
    account = str(identity.get("Account", ""))
    _require(account == ACCOUNT, "AWS account differs from FeatureForge authority")
    bucket = f"featureforge-stage6-tfstate-{account}-{REGION}"
    original_cwd = Path.cwd()
    try:
        os.chdir(ROOT)
        backend_before, state_before = qualifier._verify_backend(account)
        inventory_before = qualifier._inventory(account, RUN_ID)
    finally:
        os.chdir(original_cwd)
    s3 = executor._client("s3")
    versions_before = executor._version_inventory(s3, bucket, LEASE_KEY)
    _require(
        len(versions_before["versions"]) == 2,
        "lease history is not exactly original plus successor",
    )
    historical_matches = [
        version
        for version in versions_before["versions"]
        if fingerprint(version) == executor.HISTORICAL_LEASE_VERSION_FINGERPRINT
    ]
    _require(len(historical_matches) == 1, "authorized historical lease version is absent")
    historical_body = _read_version(s3, bucket, historical_matches[0])
    _require(
        sha256_bytes(historical_body) == executor.HISTORICAL_LEASE_OBJECT_SHA256,
        "historical lease bytes differ",
    )
    latest_version = versions_before["latest"]
    _require(latest_version != historical_matches[0], "successor lease is not latest")
    successor_body = _read_version(s3, bucket, latest_version)
    _require(
        sha256_bytes(successor_body) == SUCCESSOR_LEASE_OBJECT_SHA256,
        "successor lease bytes differ",
    )
    successor_value = _decode_object(successor_body, "successor lease")
    lease = successor_lease_proof(successor_value, plan_created_at=plan_created_at)
    try:
        os.chdir(ROOT)
        backend_after, state_after = qualifier._verify_backend(account)
        inventory_after = qualifier._inventory(account, RUN_ID)
    finally:
        os.chdir(original_cwd)
    versions_after = executor._version_inventory(s3, bucket, LEASE_KEY)
    _require(
        backend_after == backend_before and state_after == state_before,
        "backend state changed",
    )
    _require(inventory_after == inventory_before, "residual inventory changed")
    _require(versions_after == versions_before, "lease version history changed during finalization")
    return {
        "account": account,
        "backend": backend_before,
        "inventory": inventory_before,
        "lease": lease,
        "lease_version_count": len(versions_before["versions"]),
        "successor_body": successor_body,
        "successor_version": latest_version,
    }


def finalize(args: argparse.Namespace) -> dict[str, Any]:
    _require(args.confirm == CONFIRMATION, "explicit read-only confirmation is absent")
    _require(args.qualification_run_id == QUALIFICATION_RUN_ID, "qualification run differs")
    _require(
        args.qualification_receipt_sha256 == QUALIFICATION_RECEIPT_SHA256,
        "qualification receipt digest differs",
    )
    source_root = args.source_root.resolve()
    failed_output = args.failed_output.resolve()
    output = args.output_dir.resolve()
    _require(not output.is_relative_to(ROOT.resolve()), "output must be outside the repository")
    _require(
        not output.is_relative_to(failed_output),
        "output must not alter the failed executor data",
    )
    _require_clean_exact_source(source_root)
    recovery_commit = _git(ROOT, "rev-parse", "HEAD")
    recovery_tree = _git(ROOT, "rev-parse", "HEAD^{tree}")
    _require(not _git(ROOT, "status", "--porcelain"), "recovery executor worktree is not clean")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    private = output / "private"
    public = output / "public"
    private.mkdir(mode=0o700)
    public.mkdir(mode=0o700)
    binary_plan, raw_plan, variables = _validate_private_inputs(failed_output, source_root)
    artifacts = _validate_artifacts(source_root, failed_output, private)
    normalized = _validate_terraform_saved_plan(
        args.terraform.resolve(),
        source_root,
        failed_output,
        binary_plan,
        raw_plan,
        private,
    )
    plan_created_at = _parse_timestamp(PLAN_TIMESTAMP)
    run_metadata = executor._github_run(args.qualification_run_id)
    qualification_completed_at = executor.validate_qualification_run(
        run_metadata,
        source_commit=SOURCE_COMMIT,
        observed_at_epoch=plan_created_at,
    )
    live = _live_read_only_proofs(plan_created_at)
    lease = live["lease"]
    expires_at = historical_plan_window(
        qualification_completed_at=qualification_completed_at,
        plan_created_at=plan_created_at,
        lease_acquired_at=lease["acquired_at_epoch"],
        lease_expires_at=lease["expires_at_epoch"],
    )
    inventory_digest = sha256_bytes(canonical_json(live["inventory"]).encode())
    authority = PlanAuthority(
        source_commit=SOURCE_COMMIT,
        source_tree=SOURCE_TREE,
        variable_digest=_sha_file(variables),
        provider_lock_digest=_sha_file(source_root / "infra/terraform/.terraform.lock.hcl"),
        state_lineage_fingerprint=live["backend"]["state"]["lineage_fingerprint"],
        state_serial=live["backend"]["state"]["serial"],
        account_fingerprint=fingerprint(live["account"]),
        region=REGION,
        artifact_digests=artifacts,
        inventory_digest=inventory_digest,
        lease_digest=lease["digest"],
        binary_plan_sha256=BINARY_PLAN_SHA256,
        normalized_plan_sha256=normalized["normalized_plan_sha256"],
        created_at_epoch=plan_created_at,
        expires_at_epoch=expires_at,
    )
    lease_receipt = {
        "contract": "stage6-lease-successor-recovery-receipt-v1",
        "historical_lease_preserved": True,
        "lease_digest": lease["digest"],
        "lease_object_sha256": SUCCESSOR_LEASE_OBJECT_SHA256,
        "lease_version_count": live["lease_version_count"],
        "owner": RUN_ID,
        "source_commit": SOURCE_COMMIT,
        "successor_version_fingerprint": fingerprint(live["successor_version"]),
        "successor_write_count": 1,
    }
    no_mutation = {
        "aws_writes_executed_by_finalizer": False,
        "contract": "stage6-saved-plan-finalization-no-mutation-v1",
        "lease_renewed": False,
        "managed_workload_executed": False,
        "new_terraform_plan_executed": False,
        "qualification_receipt_sha256": args.qualification_receipt_sha256,
        "qualification_run_id": args.qualification_run_id,
        "runtime_resource_mutation_executed": False,
        "source_commit": SOURCE_COMMIT,
        "state_bytes_unchanged": True,
        "state_serial": live["backend"]["state"]["serial"],
        "terraform_apply_executed": False,
        "terraform_destroy_executed": False,
        "terraform_import_executed": False,
        "terraform_show_reproduced_saved_json": True,
        "terraform_validate_executed": True,
    }
    report = {
        "binary_plan_sha256": BINARY_PLAN_SHA256,
        "contract": "stage6-saved-plan-finalization-report-v1",
        "current_execution_authority": False,
        "evidence_kind": "HISTORICAL_SAVED_PLAN_RECOVERY",
        "failed_executor_public_file_count": 0,
        "finalized_at_epoch": int(time.time()),
        "historical_plan_authority": authority.as_dict(),
        "lifecycle_resource_count": 3,
        "plan_timestamp": PLAN_TIMESTAMP,
        "plan_was_inside_original_authority_windows": True,
        "raw_plan_sha256": RAW_PLAN_SHA256,
        "recovery_executor_commit": recovery_commit,
        "recovery_executor_tree": recovery_tree,
        "resource_count": normalized["resource_count"],
        "source_commit": SOURCE_COMMIT,
        "source_tree": SOURCE_TREE,
    }
    report["receipt_sha256"] = digest(report)
    _write_public(public / "normalized-plan.json", normalized)
    _write_public(public / "plan-authority.json", authority.as_dict())
    _write_public(public / "lease-successor-recovery-receipt.json", lease_receipt)
    _write_public(public / "no-mutation-receipt.json", no_mutation)
    _write_public(public / "finalization-report.json", report)
    manifest = {
        "contract": "stage6-saved-plan-public-evidence-manifest-v1",
        "files_sha256": {
            path.name: _sha_file(path) for path in sorted(public.iterdir()) if path.is_file()
        },
        "project": qualifier.PROJECT,
        "source_commit": SOURCE_COMMIT,
        "stage": 6,
    }
    manifest["manifest_sha256"] = digest(manifest)
    _write_public(public / "manifest.json", manifest)
    archive = output / "featureforge-stage6-saved-plan-public.zip"
    _public_archive(public, archive)
    return {
        "archive": str(archive),
        "archive_sha256": _sha_file(archive),
        "aws_write_count": 0,
        "current_execution_authority": False,
        "new_terraform_plan_count": 0,
        "plan_authority_sha256": _sha_file(public / "plan-authority.json"),
        "source_commit": SOURCE_COMMIT,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--failed-output", required=True, type=Path)
    parser.add_argument("--terraform", required=True, type=Path)
    parser.add_argument("--qualification-run-id", required=True, type=int)
    parser.add_argument("--qualification-receipt-sha256", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--confirm", required=True)
    args = parser.parse_args()
    try:
        result = finalize(args)
    except (
        KeyError,
        LiveEvidenceError,
        SavedPlanFinalizationError,
        TypeError,
        ValueError,
    ) as error:
        print(f"FEATUREFORGE_STAGE6_SAVED_PLAN_FINALIZATION=FAIL ({type(error).__name__}: {error})")
        return 1
    except (
        BotoCoreError,
        ClientError,
        json.JSONDecodeError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"FEATUREFORGE_STAGE6_SAVED_PLAN_FINALIZATION=FAIL ({type(error).__name__})")
        return 1
    print("FEATUREFORGE_STAGE6_SAVED_PLAN_FINALIZATION=PASS")
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
