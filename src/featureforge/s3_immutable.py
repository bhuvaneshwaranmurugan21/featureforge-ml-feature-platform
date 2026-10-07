"""Bounded, versioned S3 publication shared by Stage 6 runtimes."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from typing import Any

from featureforge.managed import ManagedContractError, S3ObjectAuthority

MAX_OBJECT_BYTES = 32 * 1024 * 1024
MAX_PUBLISH_ATTEMPTS = 3


def _checksum(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")


def _bounded_body(value: Any, *, maximum: int) -> bytes:
    if isinstance(value, bytes):
        body = value
    else:
        reader = getattr(value, "read", None)
        if not callable(reader):
            raise ManagedContractError("S3 body is not readable")
        try:
            body = reader(maximum + 1)
        finally:
            closer = getattr(value, "close", None)
            if callable(closer):
                closer()
    if not isinstance(body, bytes) or len(body) > maximum:
        raise ManagedContractError("S3 object exceeds the bounded byte limit")
    return body


def _error_code(error: Exception) -> str:
    response = getattr(error, "response", {})
    if isinstance(response, Mapping):
        detail = response.get("Error", {})
        if isinstance(detail, Mapping):
            return str(detail.get("Code", ""))
    return ""


def _write_response(response: Mapping[str, Any], body: bytes, kms_key: str) -> str:
    version = response.get("VersionId")
    if not isinstance(version, str) or not version or version == "null":
        raise ManagedContractError("immutable output has no non-null VersionId")
    if response.get("ChecksumSHA256") != _checksum(body):
        raise ManagedContractError("immutable output lacks its exact S3 SHA256 checksum")
    if response.get("ServerSideEncryption") != "aws:kms":
        raise ManagedContractError("immutable output is not encrypted with KMS")
    if response.get("SSEKMSKeyId") != kms_key:
        raise ManagedContractError("immutable output has a different KMS key")
    return version


def _reconcile_existing(
    s3: Any, *, bucket: str, key: str, body: bytes, owner: str, kms_key: str
) -> S3ObjectAuthority | None:
    try:
        response = s3.get_object(
            Bucket=bucket, Key=key, ExpectedBucketOwner=owner, ChecksumMode="ENABLED"
        )
    except Exception as error:
        if _error_code(error) in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    observed = _bounded_body(response.get("Body"), maximum=len(body))
    if observed != body:
        raise ManagedContractError("immutable output key already contains different bytes")
    version = _write_response(response, body, kms_key)
    # Pin the recovered version explicitly: never return an unversioned latest read.
    exact = s3.get_object(
        Bucket=bucket,
        Key=key,
        VersionId=version,
        ExpectedBucketOwner=owner,
        ChecksumMode="ENABLED",
    )
    if _bounded_body(exact.get("Body"), maximum=len(body)) != body:
        raise ManagedContractError("recovered immutable output bytes changed")
    if _write_response(exact, body, kms_key) != version:
        raise ManagedContractError("recovered immutable output version changed")
    return S3ObjectAuthority(bucket, key, version, hashlib.sha256(body).hexdigest())


def put_immutable_output(
    s3: Any, *, bucket: str, key: str, body: bytes, owner: str, kms_key: str
) -> S3ObjectAuthority:
    """Create once, or reconcile an identical prior write after an ambiguous ack.

    Retries retain If-None-Match; permission and other terminal errors are never
    converted into success. Conflicting bytes fail closed, not overwrite.
    """
    if not isinstance(body, bytes) or len(body) > MAX_OBJECT_BYTES:
        raise ManagedContractError("Glue output exceeds the bounded byte limit")
    S3ObjectAuthority(bucket, key, "preflight", hashlib.sha256(body).hexdigest())
    for attempt in range(MAX_PUBLISH_ATTEMPTS):
        try:
            response = s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ExpectedBucketOwner=owner,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=kms_key,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=_checksum(body),
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except Exception as error:
            retryable = _error_code(error) in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
                "412",
                "409",
            } or type(error).__name__ in {
                "ReadTimeoutError",
                "ConnectTimeoutError",
                "ConnectionClosedError",
                "EndpointConnectionError",
                "TimeoutError",
            }
            if not retryable:
                raise
            existing = _reconcile_existing(
                s3, bucket=bucket, key=key, body=body, owner=owner, kms_key=kms_key
            )
            if existing is not None:
                return existing
            if attempt + 1 == MAX_PUBLISH_ATTEMPTS:
                raise ManagedContractError(
                    "bounded immutable publication retries exhausted"
                ) from error
        else:
            version = _write_response(response, body, kms_key)
            return S3ObjectAuthority(bucket, key, version, hashlib.sha256(body).hexdigest())
    raise ManagedContractError("immutable publication did not produce an authority")
