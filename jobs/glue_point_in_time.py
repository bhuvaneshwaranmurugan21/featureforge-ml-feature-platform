"""Bounded AWS Glue entry point for the verified point-in-time computation."""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import sys
from collections.abc import Mapping
from typing import Any

from featureforge.canonical import canonical_json, digest
from featureforge.managed import ManagedContractError, S3ObjectAuthority
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.spark_runtime import full_rebuild

PAYLOAD_FIELDS = {
    "customer_ids",
    "definitions",
    "event_cutoff",
    "events",
    "generation_id",
    "knowledge_cutoff",
}
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
        body = reader(maximum + 1)
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


def publish_outputs(
    s3: Any,
    *,
    input_authority: S3ObjectAuthority,
    generation_id: str,
    rows_bytes: bytes,
    manifest_bytes: bytes,
    bucket: str,
    prefix: str,
    owner: str,
    kms_key: str,
    launch_authority_digest: str,
) -> dict[str, Any]:
    """Publish the commit receipt only after both exact output authorities exist."""
    if (
        not isinstance(launch_authority_digest, str)
        or len(launch_authority_digest) != 64
        or any(value not in "0123456789abcdef" for value in launch_authority_digest)
    ):
        raise ManagedContractError("Glue output requires exact launch authority")
    if not prefix.endswith("/"):
        raise ManagedContractError("Glue output prefix must end with a slash")
    if len(rows_bytes) > MAX_OBJECT_BYTES or len(manifest_bytes) > MAX_OBJECT_BYTES:
        raise ManagedContractError("Glue output exceeds the bounded byte limit")
    rows_value = json.loads(rows_bytes)
    manifest_value = json.loads(manifest_bytes)
    if not isinstance(rows_value, list) or not isinstance(manifest_value, dict):
        raise ManagedContractError("Glue output rows and manifest have invalid shapes")
    if (
        manifest_value.get("generation_id") != generation_id
        or manifest_value.get("managed_rows_sha256") != hashlib.sha256(rows_bytes).hexdigest()
        or manifest_value.get("managed_rows_digest") != digest(rows_value)
        or manifest_value.get("row_count") != len(rows_value)
    ):
        raise ManagedContractError("Glue output manifest does not bind its generation and rows")
    kwargs = {"s3": s3, "bucket": bucket, "owner": owner, "kms_key": kms_key}
    rows = put_immutable_output(**kwargs, key=f"{prefix}rows.json", body=rows_bytes)
    manifest = put_immutable_output(**kwargs, key=f"{prefix}manifest.json", body=manifest_bytes)
    receipt_body = {
        "contract": "stage6-glue-output-authority-v2",
        "launch_authority_digest": launch_authority_digest,
        "generation_id": generation_id,
        "input_authority": input_authority.as_dict(),
        "manifest_authority": manifest.as_dict(),
        "rows_authority": rows.as_dict(),
    }
    receipt = receipt_body | {"authority_digest": digest(receipt_body)}
    receipt_bytes = (canonical_json(receipt) + "\n").encode("utf-8")
    authority = put_immutable_output(
        **kwargs, key=f"{prefix}output-authority.json", body=receipt_bytes
    )
    return {"output_authority": authority.as_dict(), "receipt": receipt}


def execute_payload(
    spark: Any,
    payload: Mapping[str, Any],
    *,
    max_input_rows: int,
    max_output_rows: int,
) -> tuple[bytes, bytes]:
    if set(payload) != PAYLOAD_FIELDS:
        raise ManagedContractError("Glue input has missing or unexpected fields")
    events_value = payload["events"]
    definitions_value = payload["definitions"]
    customers_value = payload["customer_ids"]
    if not isinstance(events_value, list) or len(events_value) > max_input_rows:
        raise ManagedContractError("Glue input exceeds the row limit")
    if not isinstance(definitions_value, list) or not isinstance(customers_value, list):
        raise ManagedContractError("Glue definitions and customers must be lists")
    definitions = tuple(FeatureDefinition(**row) for row in definitions_value)
    events = tuple(PaymentEvent(**row) for row in events_value)
    build = full_rebuild(
        spark,
        str(payload["generation_id"]),
        definitions,
        events,
        tuple(str(item) for item in customers_value),
        event_cutoff=int(payload["event_cutoff"]),
        knowledge_cutoff=int(payload["knowledge_cutoff"]),
    )
    if len(build.values) > max_output_rows:
        raise ManagedContractError("Glue output exceeds the row limit")
    rows = tuple(value.as_dict() for value in build.values)
    rows_bytes = (canonical_json(rows) + "\n").encode("utf-8")
    manifest = dict(build.manifest) | {
        "managed_rows_sha256": hashlib.sha256(rows_bytes).hexdigest(),
        "managed_rows_digest": digest(rows),
    }
    manifest_bytes = (canonical_json(manifest) + "\n").encode("utf-8")
    return rows_bytes, manifest_bytes


def _arguments(argv: list[str]) -> dict[str, str]:
    required = (
        "input-bucket",
        "input-key",
        "input-version",
        "input-sha256",
        "output-bucket",
        "output-prefix",
        "kms-key-arn",
        "expected-bucket-owner",
        "max-input-rows",
        "max-output-rows",
        "launch-authority-digest",
    )
    result: dict[str, str] = {}
    iterator = iter(argv)
    for item in iterator:
        if not item.startswith("--"):
            continue
        try:
            result[item[2:]] = next(iterator)
        except StopIteration as error:
            raise ManagedContractError(f"argument has no value: {item}") from error
    missing = set(required) - set(result)
    if missing:
        raise ManagedContractError(f"missing Glue arguments: {sorted(missing)}")
    return result


def main() -> None:  # pragma: no cover - managed runtime boundary
    args = _arguments(sys.argv[1:])
    boto3: Any = importlib.import_module("boto3")
    spark_module: Any = importlib.import_module("pyspark.sql")
    authority = S3ObjectAuthority(
        bucket=args["input-bucket"],
        key=args["input-key"],
        version_id=args["input-version"],
        sha256=args["input-sha256"],
    )
    s3 = boto3.client("s3")
    response = s3.get_object(
        Bucket=authority.bucket,
        Key=authority.key,
        VersionId=authority.version_id,
        ExpectedBucketOwner=args["expected-bucket-owner"],
    )
    body = _bounded_body(response["Body"], maximum=MAX_OBJECT_BYTES)
    if response.get("VersionId") != authority.version_id:
        raise ManagedContractError("Glue source version changed")
    if hashlib.sha256(body).hexdigest() != authority.sha256:
        raise ManagedContractError("Glue source checksum changed")
    payload = json.loads(body)
    spark = spark_module.SparkSession.builder.getOrCreate()
    rows_bytes, manifest_bytes = execute_payload(
        spark,
        payload,
        max_input_rows=int(args["max-input-rows"]),
        max_output_rows=int(args["max-output-rows"]),
    )
    published = publish_outputs(
        s3,
        input_authority=authority,
        generation_id=str(payload["generation_id"]),
        rows_bytes=rows_bytes,
        manifest_bytes=manifest_bytes,
        bucket=args["output-bucket"],
        prefix=args["output-prefix"],
        owner=args["expected-bucket-owner"],
        kms_key=args["kms-key-arn"],
        launch_authority_digest=args["launch-authority-digest"],
    )
    print("FEATUREFORGE_GLUE_OUTPUT_AUTHORITY=" + canonical_json(published))


if __name__ == "__main__":
    main()
