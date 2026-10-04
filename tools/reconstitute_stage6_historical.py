"""Reconstitute exact historical public proofs without AWS or Terraform execution."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import types
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from featureforge.canonical import canonical_json, digest
from featureforge.managed import PlanAuthority
from featureforge.stage6_live import LiveEvidenceError, fingerprint, sha256_bytes

SOURCE = "60c9fb508548470943ba6c66cec6774cc86ad3c0"
TREE = "66ab91cefa34fd74403750f4d63d193861a5b289"
OWNER = "s6-plan-20260930"
ACCOUNT = "857229544428"
REGION = "ap-southeast-2"
QUALIFIED_AT = 1790915836
QUALIFICATION_RUN = 36848742228
QUALIFICATION_SHA = "f355606d469da814fa51f4831c942e46e02bd49ab2080a29fb0e2d1fd8732b70"
INVENTORY_SHA = "a0c1ad01479fa02c11bf8c8f615f3d768630960ae4306c463afb19da1fe697cb"
STATE_SHA = "923d1161b365158a847c44e4ffe6dedfe7fd5be8bd5bca73c64dcf6a3faebefc"
LINEAGE_SHA = "46bc30012efc3425bc4de01868757715fad0f233a9371d6f6bbca85e857e60f5"
STATE_VERSION_SHA = "0e3458b8886701d255aa424a05fe9b1336759d6397c201baf87be361c6fa60ed"
LOCK_SHA = "c964a1196cbe49c7d819149779403f5a484d74f967a78053fefa480ee1359b7d"
ARTIFACT_SHAS = {
    "featureforge-control-worker.zip":
        "cdd2cd398e8b3fddf86d3b751b1aebc4d40556b7bd6549f06ef720109547784c",
    "featureforge-glue-library.zip":
        "ae8a87179fd7e67935004035fb3af185eab17b35b737a0395f53789d560520f1",
}
ORIGINAL_SHAS = {
    "BINARY_PLAN_SHA256":
        "870bab9703dc47beab380caada639bb26315e1614100d8fadad1044f08f3e56c",
    "NORMALIZED_PLAN_SHA256":
        "77b98688bd783135e1581b4607015b4dc341f518613917b68d083609d1727286",
    "PLAN_AUTHORITY_RECEIPT_SHA256":
        "7dabcc7cb9d54a5680de819700b1d1f7daa7fb16b63922cbb106429ecd57ba73",
    "NO_MUTATION_RECEIPT_SHA256":
        "9a47ec10915025ebc30c8361bcebafa134a847d7fceecc3967c49dbd2838aa95",
}
CHECK_NAMES = (
    "all_actions_allowlisted", "event_schedule_disabled", "glue_concurrency_one",
    "glue_timeout_fifteen_minutes", "glue_two_g1x_workers", "lambda_concurrency_one",
    "control_table_pitr", "online_table_pitr", "online_table_ttl",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise LiveEvidenceError(message)


def verify_witness(text: str) -> None:
    """Partial terminal transcripts supply digest/success witnesses, not full logs."""
    for key, expected in ORIGINAL_SHAS.items():
        values = re.findall(rf"^{re.escape(key)}=(\S+)$", text, re.MULTILINE)
        require(values == [expected], f"original digest witness missing or duplicate: {key}")
    for marker in (
        "NORMALIZED_PLAN_VALID=TRUE", "RESOURCE_COUNT=42", "STATE_UNCHANGED=TRUE",
        "LEASE_SINGLE_VERSION=TRUE", "STAGE6_LEASE_AND_PLAN_TRANSACTION=SUCCESS",
        "NO_TERRAFORM_APPLY=TRUE", "NO_STATE_MUTATION=TRUE", "NO_RUNTIME_RESOURCE_MUTATION=TRUE",
    ):
        require(len(re.findall(rf"^{re.escape(marker)}$", text, re.MULTILINE)) == 1,
                f"original success witness missing or duplicate: {marker}")
    require("Plan: 42 to add, 0 to change, 0 to destroy." in text, "plan summary missing")


def verify_collector(value: Mapping[str, Any]) -> None:
    payload = dict(value)
    checksum = payload.pop("receipt_sha256", None)
    require(checksum == digest(payload), "collector receipt digest mismatch")
    expected = {
        "contract": "stage6-read-only-recovery-observation-v1",
        "evidence_kind": "LIVE_READ_ONLY_OBSERVATION",
        "historical_plan_source_commit": SOURCE, "historical_plan_source_tree": TREE,
        "region": REGION, "historical_evidence_only": True,
        "current_execution_authority": False, "lease_single_version": True,
        "state_single_version": True, "pre_post_versions_unchanged": True,
        "aws_writes_executed": False, "terraform_executed": False, "lease_renewed": False,
        "state_version_fingerprint": STATE_VERSION_SHA,
    }
    for field, target in expected.items():
        require(type(value.get(field)) is type(target) and value[field] == target,
                f"collector binding mismatch: {field}")
    require(bool(re.fullmatch(r"[0-9a-f]{40}", str(value.get("executor_commit", "")))),
            "collector executor is not an exact commit")
    require(value["state"] == {
        "lineage_fingerprint": LINEAGE_SHA, "serial": 0, "sha256": STATE_SHA,
        "terraform_version": "1.9.8",
    }, "collector state proof mismatch")
    identity = value["identity"]
    require(identity.get("verified") is True, "collector identity was not verified")
    require(identity.get("account_fingerprint") == fingerprint(ACCOUNT), "wrong collector account")
    require(identity.get("role_fingerprint") == fingerprint("FeatureForgeGitHubOidcRole"),
            "wrong collector role")
    lease = value["lease"]
    require(lease.get("owner") == OWNER and lease.get("source_commit") == SOURCE,
            "collector lease identity mismatch")
    for field in ("acquired_at_epoch", "heartbeat_at_epoch", "expires_at_epoch"):
        require(type(lease.get(field)) is int, "collector lease epoch is not an integer")
    require(lease["acquired_at_epoch"] == lease["heartbeat_at_epoch"], "lease heartbeat drift")
    require(lease["expires_at_epoch"] - lease["acquired_at_epoch"] == 3300, "lease horizon drift")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", str(lease.get("digest", "")))),
            "lease digest invalid")
    observed = value.get("observed_at_epoch")
    require(type(observed) is int and observed >= lease["acquired_at_epoch"],
            "collector observation epoch invalid")
    require(value.get("lease_expired_at_observation") is (observed >= lease["expires_at_epoch"]),
            "collector expiry classification mismatch")


def normalize_source(files: Mapping[str, str]) -> dict[str, Any]:
    rows = []
    for text in files.values():
        for resource_type, name in re.findall(r'^resource "([^"]+)" "([^"]+)"', text,
                                              re.MULTILINE):
            keys = ("artifacts", "offline", "evidence") if (
                name == "managed" and resource_type.startswith("aws_s3_bucket_")
            ) else (None,)
            for key in keys:
                address = f"{resource_type}.{name}"
                if key is not None:
                    address += f"[{json.dumps(key)}]"
                rows.append({"actions": ["create"], "address": address,
                             "name": name, "type": resource_type})
    require(len(rows) == 42 and len({row["address"] for row in rows}) == 42,
            "pinned source resource graph differs")
    normalized = {
        "action_counts": {"create": 42}, "checks": {name: True for name in CHECK_NAMES},
        "format_version": "1.2", "resource_changes": sorted(rows, key=lambda row: row["address"]),
        "resource_count": 42, "terraform_version": "1.9.8",
    }
    checksum = sha256_bytes((canonical_json(normalized) + "\n").encode())
    require(checksum == ORIGINAL_SHAS["NORMALIZED_PLAN_SHA256"],
            "normalized derivative does not match exact original digest")
    normalized["normalized_plan_sha256"] = checksum
    return normalized


def no_mutation_receipt() -> dict[str, Any]:
    receipt = {
        "contract": "stage6-plan-no-mutation-receipt-v1", "exclusive_lease_object_versions": 1,
        "lease_overwrite_executed": False, "managed_workload_executed": False,
        "qualification_receipt_sha256": QUALIFICATION_SHA,
        "qualification_run_id": QUALIFICATION_RUN,
        "runtime_resource_mutation_executed": False, "source_commit": SOURCE, "source_tree": TREE,
        "state_bytes_unchanged": True, "state_serial": 0, "state_sha256": STATE_SHA,
        "state_version_fingerprint": STATE_VERSION_SHA, "terraform_apply_executed": False,
        "terraform_destroy_executed": False, "terraform_import_executed": False,
        "terraform_plan_lock_disabled": True, "terraform_refresh_enabled": True,
    }
    require(digest(receipt) == ORIGINAL_SHAS["NO_MUTATION_RECEIPT_SHA256"],
            "no-mutation receipt does not match original digest")
    receipt["receipt_sha256"] = ORIGINAL_SHAS["NO_MUTATION_RECEIPT_SHA256"]
    return receipt


def authority_receipt(
    collector: Mapping[str, Any], authority_type: type[PlanAuthority] = PlanAuthority
) -> dict[str, Any]:
    verify_collector(collector)
    lease = collector["lease"]
    variables = {
        "aws_region": REGION, "environment": "stage6", "run_id": OWNER,
        "github_oidc_provider_arn": (
            f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"
        ),
        "runtime_execution_enabled": False,
    }
    bindings = {
        "source_commit": SOURCE, "source_tree": TREE,
        "variable_digest": sha256_bytes((canonical_json(variables) + "\n").encode()),
        "provider_lock_digest": LOCK_SHA, "state_lineage_fingerprint": LINEAGE_SHA,
        "state_serial": 0, "account_fingerprint": fingerprint(ACCOUNT), "region": REGION,
        "artifact_digests": tuple(sorted(ARTIFACT_SHAS.values())),
        "inventory_digest": INVENTORY_SHA,
        "lease_digest": lease["digest"], "binary_plan_sha256": ORIGINAL_SHAS["BINARY_PLAN_SHA256"],
        "normalized_plan_sha256": ORIGINAL_SHAS["NORMALIZED_PLAN_SHA256"],
    }
    end = min(lease["expires_at_epoch"], QUALIFIED_AT + 3600)
    require(0 < end - lease["acquired_at_epoch"] <= 3300, "authority search interval invalid")
    matches = []
    for created in range(lease["acquired_at_epoch"], end):
        authority = authority_type(**bindings, created_at_epoch=created,
                                  expires_at_epoch=min(lease["expires_at_epoch"], created + 3600))
        value = authority.as_dict()
        if digest(value) == ORIGINAL_SHAS["PLAN_AUTHORITY_RECEIPT_SHA256"]:
            matches.append(value)
    require(len(matches) == 1, "original authority digest has no unique matching historical proof")
    result = matches[0]
    result["authority_receipt_sha256"] = ORIGINAL_SHAS["PLAN_AUTHORITY_RECEIPT_SHA256"]
    return result


def pinned_files(repository: Path) -> dict[str, str]:
    def show(path: str) -> bytes:
        return subprocess.check_output(["git", "-C", str(repository), "show", f"{SOURCE}:{path}"])

    tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", f"{SOURCE}^{{tree}}"], text=True
    ).strip()
    require(tree == TREE, "historical source tree mismatch")
    path = "src/featureforge/canonical.py"
    require(
        (repository / path).read_bytes() == show(path), "proof canonical implementation differs"
    )
    require(sha256_bytes(show("infra/terraform/.terraform.lock.hcl")) == LOCK_SHA,
            "historical provider lock differs")
    return {name: show(f"infra/terraform/{name}").decode() for name in
            ("main.tf", "compute.tf", "iam.tf", "observability.tf")}


def historical_authority_type(repository: Path) -> type[PlanAuthority]:
    """Use the authenticated historical implementation, never a changed current model."""
    source = subprocess.check_output(
        ["git", "-C", str(repository), "show", f"{SOURCE}:src/featureforge/managed.py"]
    )
    tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", f"{SOURCE}^{{tree}}"], text=True
    ).strip()
    require(tree == TREE, "historical implementation tree mismatch")
    name = "_featureforge_stage6_authenticated_historical_managed"
    require(name not in sys.modules, "historical module name already occupied")
    module = types.ModuleType(name)
    sys.modules[name] = module
    try:
        exec(compile(source, "authenticated-stage6-historical-managed", "exec"), module.__dict__)
        return module.__dict__["PlanAuthority"]
    finally:
        del sys.modules[name]


def reconstitute(repository: Path, collector: Mapping[str, Any], witness: str,
                 artifact_dir: Path) -> dict[str, dict[str, Any]]:
    verify_collector(collector)
    verify_witness(witness)
    for name, expected in ARTIFACT_SHAS.items():
        require(sha256_bytes((artifact_dir / name).read_bytes()) == expected,
                f"deterministic artifact differs from original: {name}")
    normalized = normalize_source(pinned_files(repository))
    authority = authority_receipt(collector, historical_authority_type(repository))
    report = {
        "contract": "stage6-historical-proof-reconstitution-v1", "project": "featureforge",
        "historical_source_commit": SOURCE, "historical_source_tree": TREE,
        "collector_receipt_sha256": collector["receipt_sha256"],
        "recovery_executor_commit": collector["executor_commit"],
        "terminal_witness_sha256": sha256_bytes(witness.encode()),
        "terminal_witness_is_full_execution_log": False, "historical_evidence_only": True,
        "current_execution_authority": False, "private_binary_plan_recovered": False,
        "terraform_executed": False, "aws_writes_executed": False,
        "original_normalized_digest_matched": True, "original_authority_digest_matched": True,
        "original_no_mutation_digest_matched": True,
    }
    report["receipt_sha256"] = digest(report)
    return {"normalized-plan.json": normalized, "plan-authority.json": authority,
            "no-mutation-receipt.json": no_mutation_receipt(), "reconstitution-report.json": report}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--collector-receipt", type=Path, required=True)
    parser.add_argument("--terminal-witness", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    outputs = reconstitute(args.repository, json.loads(args.collector_receipt.read_text()),
                           args.terminal_witness.read_text(), args.artifact_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    require(not any((args.output_dir / name).exists() for name in outputs),
            "refusing to overwrite existing historical proof")
    for name, value in outputs.items():
        with (args.output_dir / name).open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(value) + "\n")
    print("EXACT_HISTORICAL_PUBLIC_PROOF_RECONSTITUTION=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
