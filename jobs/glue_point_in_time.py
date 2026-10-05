"""Bounded AWS Glue entry point for the verified point-in-time computation."""

from __future__ import annotations

import hashlib
import importlib
import json
import sys
from collections.abc import Mapping
from typing import Any

from featureforge.canonical import canonical_json, digest
from featureforge.managed import REGION_PATTERN, ManagedContractError, S3ObjectAuthority
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.s3_immutable import MAX_OBJECT_BYTES, _bounded_body, put_immutable_output
from featureforge.spark_runtime import full_rebuild

PAYLOAD_FIELDS = {
    "customer_ids",
    "definitions",
    "event_cutoff",
    "events",
    "generation_id",
    "knowledge_cutoff",
}


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
        "region",
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


def managed_s3_client(session: Any, region: str) -> Any:
    if not REGION_PATTERN.fullmatch(region):
        raise ManagedContractError("Glue requires an explicit managed AWS region")
    config_type: Any = importlib.import_module("botocore.config").Config
    return session.client(
        "s3",
        region_name=region,
        config=config_type(
            retries={"mode": "standard", "total_max_attempts": 1},
            connect_timeout=5,
            read_timeout=10,
        ),
    )


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
    s3 = managed_s3_client(boto3, args["region"])
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
