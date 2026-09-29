"""Bounded AWS Glue entry point for the verified point-in-time computation."""

from __future__ import annotations

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
    body = response["Body"].read()
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
    common = {
        "Bucket": args["output-bucket"],
        "ExpectedBucketOwner": args["expected-bucket-owner"],
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": args["kms-key-arn"],
        "ChecksumAlgorithm": "SHA256",
        "ContentType": "application/json",
    }
    s3.put_object(**common, Key=f"{args['output-prefix']}rows.json", Body=rows_bytes)
    s3.put_object(**common, Key=f"{args['output-prefix']}manifest.json", Body=manifest_bytes)


if __name__ == "__main__":
    main()
