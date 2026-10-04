"""Immutable Glue output protocol tests; no AWS runtime is contacted."""

from __future__ import annotations

import base64
import hashlib
import io
import json
from typing import Any

import pytest
from botocore.session import Session
from botocore.validate import validate_parameters

from featureforge.canonical import canonical_json, digest
from featureforge.managed import ManagedContractError, S3ObjectAuthority
from jobs.glue_point_in_time import (
    MAX_OBJECT_BYTES,
    _bounded_body,
    publish_outputs,
    put_immutable_output,
)

KMS = "arn:aws:kms:ap-southeast-2:857229544428:key/12345678-1234-1234-1234-123456789012"
OWNER = "857229544428"


def manifest_body(rows: bytes = b"[]\n", generation: str = "g1") -> bytes:
    return (
        canonical_json(
            {
                "generation_id": generation,
                "managed_rows_sha256": hashlib.sha256(rows).hexdigest(),
                "managed_rows_digest": digest(json.loads(rows)),
                "row_count": len(json.loads(rows)),
            }
        )
        + "\n"
    ).encode()


class ServiceError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class MemoryVersionedS3:
    """Wire-shaped storage fixture preserving conditional, versioned semantics."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.puts: list[dict[str, Any]] = []
        self.gets: list[dict[str, Any]] = []
        self.lost_ack = False
        self.fail_without_commit = 0
        self.response_override: dict[str, Any] = {}

    def _response(self, body: bytes, version: str) -> dict[str, Any]:
        return {
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode(),
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": KMS,
            "VersionId": version,
        } | self.response_override

    def put_object(self, **request: Any) -> dict[str, Any]:
        validate_parameters(
            request, Session().get_service_model("s3").operation_model("PutObject").input_shape
        )
        self.puts.append(request)
        assert request["IfNoneMatch"] == "*"
        if self.fail_without_commit:
            self.fail_without_commit -= 1
            raise TimeoutError("not committed")
        if request["Key"] in self.objects:
            raise ServiceError("PreconditionFailed")
        version = f"version-{len(self.objects) + 1}"
        self.objects[request["Key"]] = (request["Body"], version)
        if self.lost_ack:
            self.lost_ack = False
            raise TimeoutError("committed without acknowledgement")
        return self._response(request["Body"], version)

    def get_object(self, **request: Any) -> dict[str, Any]:
        validate_parameters(
            request, Session().get_service_model("s3").operation_model("GetObject").input_shape
        )
        self.gets.append(request)
        if request["Key"] not in self.objects:
            raise ServiceError("NoSuchKey")
        body, version = self.objects[request["Key"]]
        assert request.get("VersionId", version) == version
        return self._response(body, version) | {"Body": io.BytesIO(body)}


def write(client: MemoryVersionedS3, body: bytes = b'{"a":1}\n') -> S3ObjectAuthority:
    return put_immutable_output(
        client,
        bucket="offline-bucket",
        key="generations/g1/rows.json",
        body=body,
        owner=OWNER,
        kms_key=KMS,
    )


def test_exact_checksum_non_null_version_and_conditional_wire_shape() -> None:
    client = MemoryVersionedS3()
    authority = write(client)
    assert authority.version_id == "version-1"
    assert authority.sha256 == hashlib.sha256(b'{"a":1}\n').hexdigest()
    assert client.puts[0]["ExpectedBucketOwner"] == OWNER
    assert (
        client.puts[0]["ChecksumSHA256"]
        == base64.b64encode(hashlib.sha256(b'{"a":1}\n').digest()).decode()
    )


def test_identical_replay_recovers_same_version_without_overwriting() -> None:
    client = MemoryVersionedS3()
    first = write(client)
    assert write(client) == first
    assert len(client.objects) == 1
    assert len(client.puts) == 2
    assert client.gets[-1]["VersionId"] == first.version_id


def test_lost_ack_reconciles_exact_committed_version() -> None:
    client = MemoryVersionedS3()
    client.lost_ack = True
    authority = write(client)
    assert authority.version_id == "version-1"
    assert len(client.objects) == len(client.puts) == 1


def test_lost_ack_without_commit_retries_remain_conditional() -> None:
    client = MemoryVersionedS3()
    client.fail_without_commit = 1
    assert write(client).version_id == "version-1"
    assert len(client.puts) == 2
    assert all(item["IfNoneMatch"] == "*" for item in client.puts)


def test_publication_retries_have_strict_bound() -> None:
    client = MemoryVersionedS3()
    client.fail_without_commit = 3
    with pytest.raises(ManagedContractError, match="retries exhausted"):
        write(client)
    assert len(client.puts) == 3
    assert not client.objects


def test_conflicting_content_never_overwrites_or_publishes_receipt() -> None:
    client = MemoryVersionedS3()
    write(client)
    with pytest.raises(ManagedContractError, match="different bytes"):
        write(client, b'{"a":2}\n')
    assert client.objects["generations/g1/rows.json"][0] == b'{"a":1}\n'


@pytest.mark.parametrize(
    "overrides",
    [
        {"VersionId": "null"},
        {"VersionId": ""},
        {"ChecksumSHA256": None},
        {"ChecksumSHA256": hashlib.sha256(b'{"a":1}\n').hexdigest()},
        {"ServerSideEncryption": "AES256"},
        {"SSEKMSKeyId": "wrong-key"},
    ],
)
def test_incomplete_or_incorrect_write_receipt_fails_closed(overrides: dict[str, Any]) -> None:
    client = MemoryVersionedS3()
    client.response_override = overrides
    with pytest.raises(ManagedContractError):
        write(client)


def test_replay_also_rejects_wrong_storage_checksum() -> None:
    client = MemoryVersionedS3()
    write(client)
    client.response_override = {"ChecksumSHA256": "wrong"}
    with pytest.raises(ManagedContractError, match="checksum"):
        write(client)


def test_permission_denial_is_not_retried_or_reclassified() -> None:
    class Denied(MemoryVersionedS3):
        def put_object(self, **request: Any) -> dict[str, Any]:
            self.puts.append(request)
            raise ServiceError("AccessDenied")

    client = Denied()
    with pytest.raises(ServiceError, match="AccessDenied"):
        write(client)
    assert len(client.puts) == 1
    assert not client.gets


def test_commit_receipt_binds_input_and_both_exact_output_versions() -> None:
    client = MemoryVersionedS3()
    source = S3ObjectAuthority("input-bucket", "input.json", "input-v1", "a" * 64)
    kwargs = {
        "s3": client,
        "input_authority": source,
        "generation_id": "g1",
        "rows_bytes": b"[]\n",
        "manifest_bytes": manifest_body(),
        "bucket": "offline-bucket",
        "prefix": "generations/g1/",
        "owner": OWNER,
        "kms_key": KMS,
        "launch_authority_digest": "b" * 64,
    }
    result = publish_outputs(**kwargs)
    receipt = result["receipt"]
    assert receipt["input_authority"] == source.as_dict()
    assert receipt["rows_authority"]["version_id"] == "version-1"
    assert receipt["manifest_authority"]["version_id"] == "version-2"
    assert result["output_authority"]["version_id"] == "version-3"
    assert receipt["authority_digest"] == digest(
        {k: v for k, v in receipt.items() if k != "authority_digest"}
    )
    assert json.loads(client.objects["generations/g1/output-authority.json"][0]) == receipt
    assert publish_outputs(**kwargs) == result
    assert len(client.objects) == 3


def test_partial_publication_never_advertises_committed_output_authority() -> None:
    class ManifestDenied(MemoryVersionedS3):
        def put_object(self, **request: Any) -> dict[str, Any]:
            if request["Key"].endswith("manifest.json"):
                raise ServiceError("AccessDenied")
            return super().put_object(**request)

    client = ManifestDenied()
    with pytest.raises(ServiceError):
        publish_outputs(
            client,
            input_authority=S3ObjectAuthority("input-bucket", "i", "v1", "a" * 64),
            generation_id="g1",
            rows_bytes=b"[]\n",
            manifest_bytes=manifest_body(),
            bucket="offline-bucket",
            prefix="generations/g1/",
            owner=OWNER,
            kms_key=KMS,
            launch_authority_digest="b" * 64,
        )
    assert set(client.objects) == {"generations/g1/rows.json"}


def test_unbound_manifest_is_rejected_before_any_output_write() -> None:
    client = MemoryVersionedS3()
    with pytest.raises(ManagedContractError, match="does not bind"):
        publish_outputs(
            client,
            input_authority=S3ObjectAuthority("input-bucket", "i", "v1", "a" * 64),
            generation_id="g1",
            rows_bytes=b"[]\n",
            manifest_bytes=manifest_body(generation="g2"),
            bucket="offline-bucket",
            prefix="generations/g1/",
            owner=OWNER,
            kms_key=KMS,
            launch_authority_digest="b" * 64,
        )
    assert not client.puts


def test_read_and_output_limits_are_enforced_before_unbounded_consumption() -> None:
    assert _bounded_body(io.BytesIO(b"abc"), maximum=3) == b"abc"
    with pytest.raises(ManagedContractError, match="bounded byte"):
        _bounded_body(io.BytesIO(b"abcd"), maximum=3)
    client = MemoryVersionedS3()
    with pytest.raises(ManagedContractError, match="bounded byte"):
        write(client, b"a" * (MAX_OBJECT_BYTES + 1))
    assert not client.puts
