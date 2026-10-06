#!/usr/bin/env python3
"""Read immutable Stage 6 proofs; never acquire a lease or execute a plan.

An expired lease is evidence of a historical transaction, NOT current execution
authority. Only a sanitized observation receipt is persisted by this collector.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from featureforge.canonical import canonical_json, digest
from featureforge.stage6_live import (
    LiveEvidenceError,
    fingerprint,
    sanitized_identity,
    sha256_bytes,
    validate_initial_state,
    validate_lease,
)

ACCOUNT = "857229544428"
REGION = "ap-southeast-2"
ROLE = "FeatureForgeGitHubOidcRole"
BUCKET = "featureforge-stage6-tfstate-857229544428-ap-southeast-2"
LEASE_KEY = "leases/featureforge/stage6.json"
STATE_KEY = "state/stage6/terraform.tfstate"
OWNER = "s6-plan-20260930"
SOURCE_COMMIT = "60c9fb508548470943ba6c66cec6774cc86ad3c0"
SOURCE_TREE = "66ab91cefa34fd74403750f4d63d193861a5b289"
STATE_SHA256 = "923d1161b365158a847c44e4ffe6dedfe7fd5be8bd5bca73c64dcf6a3faebefc"
LINEAGE_FINGERPRINT = "46bc30012efc3425bc4de01868757715fad0f233a9371d6f6bbca85e857e60f5"
STATE_VERSION_FINGERPRINT = "0e3458b8886701d255aa424a05fe9b1336759d6397c201baf87be361c6fa60ed"
LEASE_VERSION_FINGERPRINT = "6afda4e75158fa14b748709eba4fe8a2c5a6795cfa72838de1986119022600dc"
CAPS = {LEASE_KEY: 16_384, STATE_KEY: 32_768}
READ_ACTIONS = (
    "sts:GetCallerIdentity",
    "s3:ListBucketVersions",
    "s3:GetObjectVersion",
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LiveEvidenceError(message)


def _identity(value: Mapping[str, Any]) -> dict[str, Any]:
    _require(value.get("Account") == ACCOUNT, "recovery caller account mismatch")
    arn = value.get("Arn")
    prefix = f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/"
    _require(
        isinstance(arn, str) and arn.startswith(prefix) and len(arn) > len(prefix),
        "recovery caller role mismatch",
    )
    identity = sanitized_identity(ACCOUNT, str(arn), ROLE)
    return {
        "account_fingerprint": identity["account_fingerprint"],
        "caller_arn_fingerprint": identity["caller_arn_fingerprint"],
        "role_fingerprint": fingerprint(ROLE),
        "verified": True,
    }


def _inventory(s3: Any, key: str) -> dict[str, Any]:
    _require(key in CAPS, "recovery key is not allowlisted")
    result = s3.list_object_versions(Bucket=BUCKET, Prefix=key, MaxKeys=10)
    _require(result.get("IsTruncated") is False, "version inventory is truncated")
    _require(not result.get("DeleteMarkers", []), "object has delete markers")
    versions = result.get("Versions")
    _require(isinstance(versions, list) and bool(versions), "object has no immutable versions")
    version_ids: list[str] = []
    latest: list[str] = []
    for version in versions:
        _require(isinstance(version, Mapping), "invalid version inventory")
        _require(version.get("Key") == key, "version inventory contains another key")
        version_id = version.get("VersionId")
        _require(
            isinstance(version_id, str) and bool(version_id) and version_id != "null",
            "object has no immutable version identity",
        )
        version_ids.append(version_id)
        if version.get("IsLatest") is True:
            latest.append(version_id)
        else:
            _require(version.get("IsLatest") is False, "version latest marker is invalid")
    _require(len(version_ids) == len(set(version_ids)), "version inventory contains duplicates")
    _require(len(latest) == 1, "version inventory must identify exactly one latest object")
    return {"latest": latest[0], "versions": tuple(sorted(version_ids))}


def _version_with_fingerprint(inventory: Mapping[str, Any], expected: str) -> str:
    versions = inventory.get("versions")
    _require(isinstance(versions, tuple), "normalized version inventory is invalid")
    matches = [value for value in versions if fingerprint(value) == expected]
    _require(len(matches) == 1, "required immutable object version is absent or ambiguous")
    return str(matches[0])


def _read_version(s3: Any, key: str, version_id: str) -> bytes:
    _require(key in CAPS, "recovery key is not allowlisted")
    head = s3.head_object(Bucket=BUCKET, Key=key, VersionId=version_id)
    response = s3.get_object(Bucket=BUCKET, Key=key, VersionId=version_id)
    stream = response.get("Body")
    try:
        for metadata in (head, response):
            _require(metadata.get("VersionId") == version_id, "object version read mismatch")
            _require(metadata.get("ServerSideEncryption") == "AES256", "object is not AES256")
            length = metadata.get("ContentLength")
            _require(
                type(length) is int and 0 < length <= CAPS[key],
                "object exceeds bounded recovery size",
            )
        _require(head["ContentLength"] == response["ContentLength"], "object length drift")
        _require(stream is not None, "object body is absent")
        body = stream.read(CAPS[key] + 1)
        _require(isinstance(body, bytes), "object body is not bytes")
        _require(len(body) == response["ContentLength"], "object body length mismatch")
        return body
    finally:
        if stream is not None:
            stream.close()


def _lease_proof(body: bytes, observed_at_epoch: int) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveEvidenceError("lease object is not valid JSON") from error
    fields = {
        "contract",
        "lease_id",
        "owner",
        "source_commit",
        "acquired_at_epoch",
        "heartbeat_at_epoch",
        "expires_at_epoch",
    }
    _require(isinstance(value, dict) and set(value) == fields, "lease fields differ")
    for field in ("acquired_at_epoch", "heartbeat_at_epoch", "expires_at_epoch"):
        _require(type(value[field]) is int, "lease epoch is not an integer")
    for field in ("contract", "lease_id", "owner", "source_commit"):
        _require(
            isinstance(value[field], str) and 0 < len(value[field]) <= 256,
            "lease identity is invalid",
        )
    acquired = value["acquired_at_epoch"]
    _require(value["heartbeat_at_epoch"] == acquired, "lease heartbeat was modified")
    _require(value["expires_at_epoch"] - acquired == 3300, "lease lifetime differs")
    _require(observed_at_epoch >= acquired, "observation predates acquisition")
    # Validity is checked AT ACQUISITION only; this does not renew execution authority.
    return validate_lease(
        value, source_commit=SOURCE_COMMIT, owner=OWNER, observed_at_epoch=acquired
    )


def collect(sts: Any, s3: Any, *, executor_commit: str, observed_at_epoch: int) -> dict[str, Any]:
    """Collect live read-only proofs; injected clients exist solely for unit testing."""
    _require(bool(re.fullmatch(r"[0-9a-f]{40}", executor_commit)), "invalid executor commit")
    _require(type(observed_at_epoch) is int and observed_at_epoch > 0, "invalid observation epoch")
    identity = _identity(sts.get_caller_identity())
    before = {key: _inventory(s3, key) for key in CAPS}
    state_versions = before[STATE_KEY]["versions"]
    _require(
        len(state_versions) == 1
        and fingerprint(before[STATE_KEY]["latest"]) == STATE_VERSION_FINGERPRINT,
        "state version differs from authorized bootstrap",
    )
    historical_lease_version = _version_with_fingerprint(
        before[LEASE_KEY], LEASE_VERSION_FINGERPRINT
    )
    lease_bytes = _read_version(s3, LEASE_KEY, historical_lease_version)
    state_bytes = _read_version(s3, STATE_KEY, before[STATE_KEY]["latest"])
    lease = _lease_proof(lease_bytes, observed_at_epoch)
    state = validate_initial_state(
        state_bytes, expected_lineage_fingerprint=LINEAGE_FINGERPRINT, expected_sha256=STATE_SHA256
    )
    after = {key: _inventory(s3, key) for key in CAPS}
    _require(before == after, "immutable object inventory changed during recovery")
    receipt = {
        "contract": "stage6-read-only-recovery-observation-v1",
        "evidence_kind": "LIVE_READ_ONLY_OBSERVATION",
        "executor_commit": executor_commit,
        "historical_plan_source_commit": SOURCE_COMMIT,
        "historical_plan_source_tree": SOURCE_TREE,
        "observed_at_epoch": observed_at_epoch,
        "region": REGION,
        "identity": identity,
        "lease": lease,
        "lease_object_sha256": sha256_bytes(lease_bytes),
        "lease_object_version_fingerprint": fingerprint(historical_lease_version),
        "lease_version_count": len(before[LEASE_KEY]["versions"]),
        "lease_historical_version_preserved": True,
        "lease_historical_version_latest": before[LEASE_KEY]["latest"]
        == historical_lease_version,
        "lease_successor_versions_present": len(before[LEASE_KEY]["versions"]) > 1,
        "lease_single_version": len(before[LEASE_KEY]["versions"]) == 1,
        "lease_expired_at_observation": observed_at_epoch >= lease["expires_at_epoch"],
        "historical_evidence_only": True,
        "current_execution_authority": False,
        "state": state,
        "state_version_fingerprint": fingerprint(before[STATE_KEY]["latest"]),
        "state_single_version": True,
        "pre_post_versions_unchanged": True,
        "authorized_read_actions": list(READ_ACTIONS),
        "aws_writes_executed": False,
        "terraform_executed": False,
        "lease_renewed": False,
    }
    receipt["receipt_sha256"] = digest(receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executor-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = Config(
        connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1, "mode": "standard"}
    )
    try:
        receipt = collect(
            boto3.client("sts", region_name=REGION, config=config),
            boto3.client("s3", region_name=REGION, config=config),
            executor_commit=args.executor_commit,
            observed_at_epoch=int(time.time()),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(receipt) + "\n")
    except Exception as error:
        # AWS exception strings can contain private identities. Never print them.
        print(f"STAGE6_READ_ONLY_RECOVERY=FAIL ({type(error).__name__})")
        return 1
    print("STAGE6_READ_ONLY_RECOVERY=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
