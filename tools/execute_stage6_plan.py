#!/usr/bin/env python3
"""Acquire one CAS successor lease and create a private, refresh-aware Stage 6 plan.

This command has exactly one AWS write path: an S3 PutObject guarded by If-Match
against the observed expired lease. It never invokes Terraform apply, destroy,
import, or any managed workload. Binary plans and private inputs remain outside
the repository; only sanitized receipts are packaged for external retention.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import time
import urllib.request
import zipfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from featureforge.canonical import canonical_json, digest
from featureforge.managed import LeaseSnapshot, PlanAuthority
from featureforge.stage6_live import (
    LiveEvidenceError,
    fingerprint,
    normalize_terraform_plan,
    sha256_bytes,
    validate_lease,
)
from tools import qualify_stage6_aws as qualifier
from tools.build_stage6_artifacts import build

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "857229544428"
REGION = "ap-southeast-2"
BRANCH = "part3-stage6-aws-plan-qualification"
REPOSITORY = "bhuvaneshwaranmurugan21/featureforge-ml-feature-platform"
RUN_ID = "s6-plan-20260930"
LEASE_KEY = "leases/featureforge/stage6.json"
STATE_KEY = "state/stage6/terraform.tfstate"
LEASE_CAP = 16_384
LEASE_SECONDS = 3_300
CONFIRMATION = "AUTHORIZE_ONE_CAS_LEASE_WRITE_AND_PLAN_ONLY"
HISTORICAL_SOURCE_COMMIT = "60c9fb508548470943ba6c66cec6774cc86ad3c0"
HISTORICAL_LEASE_OBJECT_SHA256 = (
    "6fd527d67013fb86225d19c54ba7e2f2dd3f04cd51b5bbfb77340dc972619099"
)
HISTORICAL_LEASE_VERSION_FINGERPRINT = (
    "6afda4e75158fa14b748709eba4fe8a2c5a6795cfa72838de1986119022600dc"
)


class PlanExecutionError(RuntimeError):
    """The bounded lease/plan transaction cannot proceed safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PlanExecutionError(message)


def _sha_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def _client(service: str, region: str = REGION) -> Any:
    return boto3.client(
        service,
        region_name=region,
        config=Config(
            retries={"mode": "standard", "total_max_attempts": 1},
            connect_timeout=5,
            read_timeout=20,
        ),
    )


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def validate_qualification_run(
    value: Mapping[str, Any], *, source_commit: str, observed_at_epoch: int
) -> int:
    """Bind the external green qualification to this exact source and a fresh window."""
    _require(value.get("name") == "Stage 6 AWS Qualification", "wrong qualification workflow")
    _require(value.get("head_sha") == source_commit, "qualification source commit differs")
    _require(value.get("head_branch") == BRANCH, "qualification branch differs")
    _require(value.get("event") == "push", "qualification was not the exact-head push gate")
    _require(value.get("status") == "completed", "qualification run is incomplete")
    _require(value.get("conclusion") == "success", "qualification run did not pass")
    updated = value.get("updated_at")
    _require(isinstance(updated, str), "qualification completion time is absent")
    try:
        completed = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        _require(completed.tzinfo is not None, "qualification completion time has no timezone")
        completed_epoch = int(completed.astimezone(UTC).timestamp())
    except (AttributeError, ValueError) as error:
        raise PlanExecutionError("qualification completion time is invalid") from error
    _require(
        0 <= observed_at_epoch - completed_epoch <= 3_600,
        "qualification is outside the one-hour plan-authority window",
    )
    return completed_epoch


def _github_run(run_id: int) -> dict[str, Any]:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{REPOSITORY}/actions/runs/{run_id}",
        headers={"Accept": "application/vnd.github+json", "User-Agent": "featureforge-stage6"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
            body = response.read(1_000_001)
    except OSError as error:
        raise PlanExecutionError("GitHub qualification metadata could not be read") from error
    _require(len(body) <= 1_000_000, "GitHub qualification metadata is oversized")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PlanExecutionError("GitHub qualification metadata is invalid") from error
    _require(isinstance(value, dict), "GitHub qualification metadata is not an object")
    return value


def _version_inventory(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    result = s3.list_object_versions(
        Bucket=bucket,
        ExpectedBucketOwner=ACCOUNT,
        Prefix=key,
        MaxKeys=100,
    )
    _require(result.get("IsTruncated") is False, "lease version inventory is truncated")
    exact_markers = [row for row in result.get("DeleteMarkers", []) if row.get("Key") == key]
    _require(not exact_markers, "lease key has a delete marker")
    exact = [row for row in result.get("Versions", []) if row.get("Key") == key]
    _require(bool(exact), "lease key has no immutable version")
    ids: list[str] = []
    latest: list[str] = []
    for row in exact:
        version_id = row.get("VersionId")
        _require(
            isinstance(version_id, str) and bool(version_id) and version_id != "null",
            "lease version identity is invalid",
        )
        ids.append(version_id)
        if row.get("IsLatest") is True:
            latest.append(version_id)
        else:
            _require(row.get("IsLatest") is False, "lease version latest marker is invalid")
    _require(len(ids) == len(set(ids)), "lease version inventory contains duplicates")
    _require(len(latest) == 1, "lease inventory must have exactly one latest version")
    return {"latest": latest[0], "versions": tuple(sorted(ids))}


def _read_latest_lease(s3: Any, bucket: str) -> tuple[dict[str, Any], bytes, str, str]:
    inventory = _version_inventory(s3, bucket, LEASE_KEY)
    version_id = inventory["latest"]
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
    finally:
        if stream is not None:
            stream.close()
    etag = head.get("ETag")
    _require(isinstance(etag, str) and bool(etag), "lease ETag is absent")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PlanExecutionError("lease body is not valid JSON") from error
    _require(isinstance(value, dict), "lease body is not an object")
    return value, body, version_id, etag


def parse_expired_lease(value: Mapping[str, Any], *, observed_at_epoch: int) -> LeaseSnapshot:
    fields = {
        "contract",
        "lease_id",
        "owner",
        "source_commit",
        "acquired_at_epoch",
        "heartbeat_at_epoch",
        "expires_at_epoch",
    }
    _require(set(value) == fields, "prior lease fields differ")
    _require(value.get("contract") == "stage6-lease-snapshot-v1", "prior lease contract differs")
    for field in ("lease_id", "owner", "source_commit"):
        _require(isinstance(value.get(field), str), f"prior lease {field} is invalid")
    for field in ("acquired_at_epoch", "heartbeat_at_epoch", "expires_at_epoch"):
        _require(type(value.get(field)) is int, f"prior lease {field} is invalid")
    lease = LeaseSnapshot(
        lease_id=value["lease_id"],
        owner=value["owner"],
        source_commit=value["source_commit"],
        acquired_at_epoch=value["acquired_at_epoch"],
        heartbeat_at_epoch=value["heartbeat_at_epoch"],
        expires_at_epoch=value["expires_at_epoch"],
    )
    _require(lease.owner == RUN_ID, "prior lease owner differs")
    _require(lease.source_commit == HISTORICAL_SOURCE_COMMIT, "prior lease source differs")
    _require(observed_at_epoch >= lease.expires_at_epoch, "prior lease is not expired")
    return lease


def successor_lease(*, source_commit: str, observed_at_epoch: int, lease_id: str) -> LeaseSnapshot:
    _require(bool(re.fullmatch(r"[0-9a-f]{40}", source_commit)), "source commit is invalid")
    _require(bool(re.fullmatch(r"[0-9a-f]{32}", lease_id)), "successor lease ID is invalid")
    return LeaseSnapshot(
        lease_id=lease_id,
        owner=RUN_ID,
        source_commit=source_commit,
        acquired_at_epoch=observed_at_epoch,
        heartbeat_at_epoch=observed_at_epoch,
        expires_at_epoch=observed_at_epoch + LEASE_SECONDS,
    )


def lease_put_request(
    *, bucket: str, previous_etag: str, body: bytes, expected_owner: str
) -> dict[str, Any]:
    _require(bool(bucket) and bool(previous_etag), "lease CAS identity is absent")
    _require(expected_owner == ACCOUNT, "lease expected owner differs")
    return {
        "Body": body,
        "Bucket": bucket,
        "ChecksumAlgorithm": "SHA256",
        "ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"),
        "ContentType": "application/json",
        "ExpectedBucketOwner": expected_owner,
        "IfMatch": previous_etag,
        "Key": LEASE_KEY,
        "ServerSideEncryption": "AES256",
    }


def _write_private(path: Path, value: str | bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(descriptor, "wb" if isinstance(value, bytes) else "w") as handle:
        handle.write(value)


def _run_private(
    command: Sequence[str],
    *,
    cwd: Path,
    log: Path,
    stdout_file: Path | None = None,
    environment_overrides: Mapping[str, str] | None = None,
) -> None:
    environment = dict(os.environ)
    environment.update(
        {"AWS_EC2_METADATA_DISABLED": "true", "CHECKPOINT_DISABLE": "1", "TF_IN_AUTOMATION": "1"}
    )
    if environment_overrides:
        environment.update(environment_overrides)
    result = subprocess.run(
        list(command),
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
    )
    _write_private(log, result.stderr if stdout_file is not None else result.stdout + result.stderr)
    if stdout_file is not None:
        _write_private(stdout_file, result.stdout)
    if result.returncode != 0:
        raise PlanExecutionError(f"private command failed: {command[0]} {command[1]}")


def terraform_commands(
    *, terraform: str, backend_file: Path, variables_file: Path, plan_file: Path
) -> tuple[list[str], list[str], list[str]]:
    init = [
        terraform,
        "-chdir=infra/terraform",
        "init",
        "-reconfigure",
        "-input=false",
        "-lockfile=readonly",
        f"-backend-config={backend_file}",
        "-no-color",
    ]
    plan = [
        terraform,
        "-chdir=infra/terraform",
        "plan",
        "-refresh=true",
        "-lock=false",
        "-input=false",
        "-no-color",
        f"-var-file={variables_file}",
        f"-out={plan_file}",
    ]
    show = [terraform, "-chdir=infra/terraform", "show", "-json", str(plan_file)]
    return init, plan, show


def _write_public(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(canonical_json(value) + "\n", encoding="utf-8")


def _public_archive(public_dir: Path, output: Path) -> None:
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(public_dir.iterdir()):
            info = zipfile.ZipInfo(path.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            info.create_system = 3
            archive.writestr(info, path.read_bytes(), compresslevel=9)


def execute(args: argparse.Namespace) -> dict[str, Any]:
    _require(args.confirm == CONFIRMATION, "explicit bounded-write confirmation is absent")
    _require(bool(re.fullmatch(r"[0-9a-f]{40}", args.source_commit)), "source commit is invalid")
    _require(
        bool(re.fullmatch(r"[0-9a-f]{64}", args.qualification_receipt_sha256)),
        "qualification receipt digest is invalid",
    )
    output = args.output_dir.resolve()
    _require(
        not output.is_relative_to(ROOT.resolve()),
        "plan output must be outside the repository",
    )
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    private = output / "private"
    public = output / "public"
    private.mkdir(mode=0o700)
    public.mkdir(mode=0o700)

    source_commit = _git("rev-parse", "HEAD")
    source_tree = _git("rev-parse", "HEAD^{tree}")
    _require(source_commit == args.source_commit, "repository HEAD differs from authorized source")
    _require(not _git("status", "--porcelain"), "repository worktree is not clean")

    observed_at = int(time.time())
    run_metadata = _github_run(args.qualification_run_id)
    qualification_completed_at = validate_qualification_run(
        run_metadata, source_commit=source_commit, observed_at_epoch=observed_at
    )
    qualification_expires_at = qualification_completed_at + 3_600

    identity = _client("sts").get_caller_identity()
    account = str(identity.get("Account", ""))
    _require(account == ACCOUNT, "AWS account differs from FeatureForge authority")
    bucket = f"featureforge-stage6-tfstate-{account}-{REGION}"
    backend_before, state_bytes_before = qualifier._verify_backend(account)
    inventory_before = qualifier._inventory(account, RUN_ID)
    _require(bool(inventory_before), "residual inventory evidence is absent")

    s3 = _client("s3")
    lease_inventory_before = _version_inventory(s3, bucket, LEASE_KEY)
    prior_value, prior_body, prior_version, prior_etag = _read_latest_lease(s3, bucket)
    _require(
        prior_version == lease_inventory_before["latest"],
        "lease changed during preflight",
    )
    _require(
        fingerprint(prior_version) == HISTORICAL_LEASE_VERSION_FINGERPRINT,
        "latest lease version is not the authorized historical version",
    )
    _require(
        sha256_bytes(prior_body) == HISTORICAL_LEASE_OBJECT_SHA256,
        "latest lease bytes are not the authorized historical lease",
    )
    prior = parse_expired_lease(prior_value, observed_at_epoch=observed_at)

    terraform = shutil.which("terraform")
    _require(terraform is not None, "Terraform is not installed")
    terraform_data = private / "terraform-data"
    terraform_data.mkdir(mode=0o700)
    terraform_environment = {"TF_DATA_DIR": str(terraform_data)}
    version_file = private / "terraform-version.json"
    _run_private(
        [terraform, "version", "-json"],
        cwd=ROOT,
        log=private / "terraform-version.err",
        stdout_file=version_file,
        environment_overrides=terraform_environment,
    )
    terraform_version = json.loads(version_file.read_text(encoding="utf-8"))
    _require(terraform_version.get("terraform_version") == "1.9.8", "Terraform version differs")

    _run_private(
        [
            terraform,
            "-chdir=infra/terraform",
            "init",
            "-backend=false",
            "-input=false",
            "-lockfile=readonly",
            "-no-color",
        ],
        cwd=ROOT,
        log=private / "terraform-local-init.log",
        environment_overrides=terraform_environment,
    )
    _run_private(
        [terraform, "-chdir=infra/terraform", "validate", "-no-color"],
        cwd=ROOT,
        log=private / "terraform-validate.log",
        environment_overrides=terraform_environment,
    )

    build_a = private / "build-a"
    build_b = private / "build-b"
    manifest_a = build(build_a)
    manifest_b = build(build_b)
    _require(manifest_a == manifest_b, "independent artifact manifests differ")
    for name in ("featureforge-control-worker.zip", "featureforge-glue-library.zip"):
        _require(
            (build_a / name).read_bytes() == (build_b / name).read_bytes(),
            f"{name} differs",
        )
    qualification = json.loads((ROOT / "evidence/stage6/artifact-qualification.json").read_text())
    expected_artifacts = {row["name"]: row["sha256"] for row in qualification["artifacts"]}
    actual_artifacts = {row["name"]: row["sha256"] for row in manifest_a["artifacts"]}
    _require(actual_artifacts == expected_artifacts, "runtime artifact evidence differs")

    acquired_at = int(time.time())
    validate_qualification_run(
        run_metadata,
        source_commit=source_commit,
        observed_at_epoch=acquired_at,
    )
    backend_prewrite, state_bytes_prewrite = qualifier._verify_backend(account)
    inventory_prewrite = qualifier._inventory(account, RUN_ID)
    _require(
        backend_prewrite == backend_before and state_bytes_prewrite == state_bytes_before,
        "backend state changed before lease acquisition",
    )
    _require(
        inventory_prewrite == inventory_before,
        "residual inventory changed before lease acquisition",
    )
    new_lease = successor_lease(
        source_commit=source_commit,
        observed_at_epoch=acquired_at,
        lease_id=secrets.token_hex(16),
    )
    lease_body = (canonical_json(new_lease.as_dict()) + "\n").encode()
    request = lease_put_request(
        bucket=bucket, previous_etag=prior_etag, body=lease_body, expected_owner=account
    )
    try:
        response = s3.put_object(**request)
    except (BotoCoreError, ClientError) as error:
        raise PlanExecutionError(
            "conditional lease write failed or has unknown outcome; "
            "do not retry without observation"
        ) from error
    new_version = response.get("VersionId")
    _require(
        isinstance(new_version, str) and bool(new_version) and new_version != "null",
        "conditional lease write returned no immutable version",
    )
    _require(
        response.get("ChecksumSHA256") == request["ChecksumSHA256"],
        "conditional lease write checksum acknowledgement differs",
    )
    lease_inventory_after = _version_inventory(s3, bucket, LEASE_KEY)
    before_versions = set(lease_inventory_before["versions"])
    after_versions = set(lease_inventory_after["versions"])
    _require(
        lease_inventory_after["latest"] == new_version
        and before_versions < after_versions
        and after_versions - before_versions == {new_version},
        "lease version history did not advance by exactly one successor",
    )
    current_value, current_body, current_version, _ = _read_latest_lease(s3, bucket)
    _require(
        current_version == new_version and current_body == lease_body,
        "successor lease differs",
    )
    current = validate_lease(
        current_value,
        source_commit=source_commit,
        owner=RUN_ID,
        observed_at_epoch=int(time.time()),
    )

    glue_script = ROOT / "jobs/glue_point_in_time.py"
    artifacts = tuple(
        sorted(
            [
                actual_artifacts["featureforge-control-worker.zip"],
                actual_artifacts["featureforge-glue-library.zip"],
                _sha_file(glue_script),
            ]
        )
    )
    provider_arn = f"arn:aws:iam::{account}:oidc-provider/token.actions.githubusercontent.com"
    tfvars = {
        "admission_authority": None,
        "aws_region": REGION,
        "control_worker_zip_path": str((build_a / "featureforge-control-worker.zip").resolve()),
        "github_oidc_provider_arn": provider_arn,
        "glue_library_zip_path": str((build_a / "featureforge-glue-library.zip").resolve()),
        "glue_script_path": str(glue_script.resolve()),
        "run_id": RUN_ID,
        "runtime_execution_enabled": False,
    }
    backend_file = private / "backend.hcl"
    variables_file = private / "stage6.auto.tfvars.json"
    _write_private(
        backend_file,
        f'bucket = "{bucket}"\nkey = "{STATE_KEY}"\nregion = "{REGION}"\nencrypt = true\n',
    )
    _write_private(variables_file, canonical_json(tfvars) + "\n")
    plan_file = private / "stage6.tfplan"
    plan_json = private / "stage6-plan.json"
    init, plan, show = terraform_commands(
        terraform=terraform,
        backend_file=backend_file,
        variables_file=variables_file,
        plan_file=plan_file,
    )
    _run_private(
        init,
        cwd=ROOT,
        log=private / "terraform-init.log",
        environment_overrides=terraform_environment,
    )
    _run_private(
        plan,
        cwd=ROOT,
        log=private / "terraform-plan.log",
        environment_overrides=terraform_environment,
    )
    _run_private(
        show,
        cwd=ROOT,
        log=private / "terraform-show.err",
        stdout_file=plan_json,
        environment_overrides=terraform_environment,
    )
    raw_plan = json.loads(plan_json.read_text())
    _require(isinstance(raw_plan, dict), "Terraform plan JSON is invalid")
    normalized = normalize_terraform_plan(raw_plan, run_id=RUN_ID)

    finished_at = int(time.time())
    _require(finished_at < new_lease.expires_at_epoch, "lease expired during planning")
    _require(
        finished_at < qualification_expires_at,
        "qualification expired during planning",
    )
    latest_value, latest_body, latest_version, _ = _read_latest_lease(s3, bucket)
    _require(
        latest_version == new_version
        and latest_body == lease_body
        and latest_value == current_value,
        "lease changed during planning",
    )
    backend_after, state_bytes_after = qualifier._verify_backend(account)
    _require(
        backend_after == backend_before and state_bytes_after == state_bytes_before,
        "backend state changed during planning",
    )
    inventory_after = qualifier._inventory(account, RUN_ID)
    _require(inventory_after == inventory_before, "residual inventory changed during planning")
    _require(not _git("status", "--porcelain"), "repository changed during planning")

    authority = PlanAuthority(
        source_commit=source_commit,
        source_tree=source_tree,
        variable_digest=_sha_file(variables_file),
        provider_lock_digest=_sha_file(ROOT / "infra/terraform/.terraform.lock.hcl"),
        state_lineage_fingerprint=backend_before["state"]["lineage_fingerprint"],
        state_serial=backend_before["state"]["serial"],
        account_fingerprint=fingerprint(account),
        region=REGION,
        artifact_digests=artifacts,
        inventory_digest=sha256_bytes(canonical_json(inventory_before).encode()),
        lease_digest=current["digest"],
        binary_plan_sha256=_sha_file(plan_file),
        normalized_plan_sha256=normalized["normalized_plan_sha256"],
        created_at_epoch=finished_at,
        expires_at_epoch=min(
            new_lease.expires_at_epoch,
            qualification_expires_at,
            finished_at + 3_600,
        ),
    )
    lease_receipt = {
        "contract": "stage6-lease-successor-receipt-v1",
        "conditional_write": "If-Match",
        "current_lease_digest": current["digest"],
        "current_version_fingerprint": fingerprint(new_version),
        "expires_at_epoch": new_lease.expires_at_epoch,
        "historical_versions_preserved": before_versions < after_versions,
        "lease_version_count_after": len(after_versions),
        "owner": RUN_ID,
        "previous_expired_at_epoch": prior.expires_at_epoch,
        "previous_lease_digest": digest(prior.as_dict()),
        "previous_lease_object_sha256": sha256_bytes(prior_body),
        "previous_version_fingerprint": fingerprint(prior_version),
        "source_commit": source_commit,
        "state_serial": backend_before["state"]["serial"],
        "successor_write_count": 1,
    }
    no_mutation = {
        "authorized_aws_writes": ["S3_CONDITIONAL_SUCCESSOR_LEASE_PUT"],
        "contract": "stage6-plan-no-mutation-receipt-v2",
        "managed_workload_executed": False,
        "qualification_receipt_sha256": args.qualification_receipt_sha256,
        "qualification_run_id": args.qualification_run_id,
        "runtime_resource_mutation_executed": False,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "state_bytes_unchanged": True,
        "state_serial": backend_before["state"]["serial"],
        "state_version_fingerprint": backend_before["state_version_fingerprint"],
        "terraform_apply_executed": False,
        "terraform_destroy_executed": False,
        "terraform_import_executed": False,
        "terraform_plan_lock_disabled": True,
        "terraform_refresh_enabled": True,
    }
    _write_public(public / "normalized-plan.json", normalized)
    _write_public(public / "plan-authority.json", authority.as_dict())
    _write_public(public / "lease-successor-receipt.json", lease_receipt)
    _write_public(public / "no-mutation-receipt.json", no_mutation)
    public_manifest = {
        "contract": "stage6-plan-public-evidence-manifest-v1",
        "files_sha256": {
            path.name: _sha_file(path) for path in sorted(public.iterdir()) if path.is_file()
        },
        "project": qualifier.PROJECT,
        "stage": 6,
    }
    public_manifest["manifest_sha256"] = digest(public_manifest)
    _write_public(public / "manifest.json", public_manifest)
    archive = output / "featureforge-stage6-plan-public.zip"
    _public_archive(public, archive)
    return {
        "archive": str(archive),
        "archive_sha256": _sha_file(archive),
        "expires_at_epoch": authority.expires_at_epoch,
        "lease_write_count": 1,
        "plan_authority_sha256": _sha_file(public / "plan-authority.json"),
        "source_commit": source_commit,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--qualification-run-id", required=True, type=int)
    parser.add_argument("--qualification-receipt-sha256", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--confirm", required=True)
    args = parser.parse_args()
    try:
        result = execute(args)
    except (PlanExecutionError, LiveEvidenceError, ValueError) as error:
        print(f"FEATUREFORGE_STAGE6_PLAN=FAIL ({type(error).__name__}: {error})")
        return 1
    except (
        BotoCoreError,
        ClientError,
        json.JSONDecodeError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"FEATUREFORGE_STAGE6_PLAN=FAIL ({type(error).__name__})")
        return 1
    print("FEATUREFORGE_STAGE6_PLAN=PASS")
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
