"""Durable fixed-slot Glue launch authority. Imports perform no AWS I/O.

This bounds physical StartJobRun calls when used with a one-attempt SDK client.
It does not bound other workflow effects or establish an aggregate cost ceiling.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from featureforge.aws_runtime import DynamoDBClient, GlueClient, RuntimeConflict, _error_code
from featureforge.canonical import canonical_json, digest
from featureforge.managed import AdmissionDenied, ManagedContractError, ManagedRunManifest

MAX_GLUE_LAUNCHES = 3


class GlueLaunchRetryable(RuntimeError):
    """A counted launch failed transiently or returned an unknown acknowledgement."""


def _key(run_id: str, suffix: str) -> dict[str, dict[str, str]]:
    return {"PK": {"S": f"RUN#{run_id}"}, "SK": {"S": f"GLUE#{suffix}"}}


def launch_request(
    manifest: ManagedRunManifest, *, job_name: str, kms_key: str, account: str
) -> dict[str, Any]:
    if job_name != f"featureforge-{manifest.namespace}-{manifest.run_id}-offline" or not kms_key:
        raise ManagedContractError("Glue launch requires exact deployment job and KMS identities")
    if len(account) != 12 or not account.isdigit():
        raise ManagedContractError("Glue launch requires an exact account identity")
    if hashlib.sha256(account.encode()).hexdigest() != manifest.account_fingerprint:
        raise AdmissionDenied("Glue deployment account differs from manifest authority")
    source = manifest.inputs[0]
    return {
        "JobName": job_name,
        "JobRunQueuingEnabled": False,
        "Timeout": 15,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Arguments": {
            "--input-bucket": source.bucket,
            "--input-key": source.key,
            "--input-version": source.version_id,
            "--input-sha256": source.sha256,
            "--output-bucket": manifest.output_bucket,
            "--output-prefix": manifest.output_prefix,
            "--kms-key-arn": kms_key,
            "--expected-bucket-owner": account,
            "--max-input-rows": str(manifest.max_input_rows),
            "--max-output-rows": str(manifest.max_output_rows),
        },
    }


class DurableGlueLauncher:
    """Never reuse a consumed slot, including after crashes or lost acknowledgements."""

    def __init__(
        self,
        database: DynamoDBClient,
        glue: GlueClient,
        *,
        table: str,
        job_name: str,
        kms_key: str,
        account: str,
    ) -> None:
        if not table:
            raise ManagedContractError("Glue launch control table is required")
        retries = getattr(getattr(getattr(glue, "meta", None), "config", None), "retries", None)
        if (
            not isinstance(retries, Mapping)
            or type(retries.get("total_max_attempts")) is not int
            or retries["total_max_attempts"] != 1
        ):
            raise ManagedContractError("Glue SDK must permit exactly one physical attempt")
        self.database = database
        self.glue = glue
        self.table = table
        self.job_name = job_name
        self.kms_key = kms_key
        self.account = account

    def _read(self, run_id: str, suffix: str) -> Mapping[str, Any] | None:
        response = self.database.get_item(
            TableName=self.table, Key=_key(run_id, suffix), ConsistentRead=True
        )
        item = response.get("Item")
        if item is not None and not isinstance(item, Mapping):
            raise RuntimeConflict("launch authority returned a malformed item")
        return item

    def start(
        self, manifest: ManagedRunManifest, *, execution_id: str, invocation_id: str
    ) -> dict[str, Any]:
        for value in (execution_id, invocation_id):
            if (
                not isinstance(value, str)
                or not value
                or len(value) > 256
                or value.strip() != value
            ):
                raise ManagedContractError("launch requires bounded execution and invocation IDs")
        request = launch_request(
            manifest, job_name=self.job_name, kms_key=self.kms_key, account=self.account
        )
        # MaxRetries is a job definition field, not a StartJobRun parameter.
        # Check the deployed job before reserving or launching any work.
        get_job = getattr(self.glue, "get_job", None)
        if not callable(get_job):
            raise ManagedContractError("Glue client cannot qualify the deployed job")
        job = get_job(JobName=self.job_name).get("Job", {})
        if (
            job.get("Name") != self.job_name
            or type(job.get("MaxRetries")) is not int
            or job["MaxRetries"] != 0
            or type(job.get("ExecutionProperty", {}).get("MaxConcurrentRuns")) is not int
            or job.get("ExecutionProperty", {}).get("MaxConcurrentRuns") != 1
        ):
            raise AdmissionDenied(
                "deployed Glue job retries or concurrency exceed launch authority"
            )
        binding = digest(
            {
                "manifest_digest": manifest.manifest_digest,
                "execution_id": execution_id,
                "request": request,
                "maximum_launches": MAX_GLUE_LAUNCHES,
            }
        )
        authority = _key(manifest.run_id, "AUTHORITY") | {
            "binding": {"S": binding},
            "contract": {"S": "stage6-glue-launch-v1"},
        }
        try:
            self.database.put_item(
                TableName=self.table,
                Item=authority,
                ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
            )
        except Exception as error:
            if _error_code(error) != "ConditionalCheckFailedException":
                raise
            if self._read(manifest.run_id, "AUTHORITY") != authority:
                raise RuntimeConflict(
                    "launch authority conflicts with immutable binding"
                ) from error
        for slot in range(1, MAX_GLUE_LAUNCHES + 1):
            suffix = f"ATTEMPT#{slot}"
            launch_token = digest({"binding": binding, "slot": slot})
            existing = self._read(manifest.run_id, suffix)
            if existing is not None:
                if existing.get("binding") != {"S": binding}:
                    raise RuntimeConflict("launch attempt conflicts with immutable binding")
                if existing.get("contract") != {"S": "stage6-glue-launch-v1"}:
                    raise RuntimeConflict("launch attempt contract is invalid")
                if existing.get("state") == {"S": "COMPLETED"}:
                    raw = existing.get("result_json", {}).get("S")
                    if not isinstance(raw, str):
                        raise RuntimeConflict("completed launch omits its result")
                    result = json.loads(raw)
                    if (
                        not isinstance(result, dict)
                        or set(result) != {"JobName", "JobRunId", "launch_authority_digest"}
                        or existing.get("result_digest") != {"S": digest(result)}
                        or result.get("JobName") != self.job_name
                        or not isinstance(result.get("JobRunId"), str)
                        or not result["JobRunId"]
                        or result.get("launch_authority_digest") != launch_token
                    ):
                        raise RuntimeConflict("completed launch result is corrupt")
                    return result
                if existing.get("state") != {"S": "RESERVED"}:
                    raise RuntimeConflict("launch attempt state is invalid")
                continue
            item = _key(manifest.run_id, suffix) | {
                "contract": {"S": "stage6-glue-launch-v1"},
                "binding": {"S": binding},
                "state": {"S": "RESERVED"},
                "invocation_id": {"S": invocation_id},
            }
            try:
                self.database.transact_write_items(
                    ClientRequestToken=digest(
                        {"binding": binding, "slot": slot, "invocation_id": invocation_id}
                    )[:32],
                    TransactItems=[
                        {
                            "ConditionCheck": {
                                "TableName": self.table,
                                "Key": _key(manifest.run_id, "AUTHORITY"),
                                "ConditionExpression": "binding = :binding",
                                "ExpressionAttributeValues": {":binding": {"S": binding}},
                            }
                        },
                        {
                            "Put": {
                                "TableName": self.table,
                                "Item": item,
                                "ConditionExpression": (
                                    "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                                ),
                            }
                        },
                    ],
                )
            except Exception as error:
                # Conditional races consume no permission. Unknown write outcomes
                # never authorize a call: their durable slot remains consumed.
                if _error_code(error) == "TransactionCanceledException":
                    winner = self._read(manifest.run_id, suffix)
                    if winner is None or winner.get("binding") != {"S": binding}:
                        raise RuntimeConflict(
                            "launch reservation has no matching winner"
                        ) from error
                    continue
                raise
            # The deployment's Glue client MUST disable SDK retries. Exactly one
            # call follows this reservation; an unknown StartJobRun result is not
            # retried in this invocation and leaves the slot permanently consumed.
            counted_request = request | {
                "Arguments": request["Arguments"] | {"--launch-authority-digest": launch_token}
            }
            try:
                response = self.glue.start_job_run(**counted_request)
            except Exception as error:
                if (
                    _error_code(error)
                    in {
                        "ThrottlingException",
                        "ConcurrentRunsExceededException",
                        "OperationTimeoutException",
                        "InternalServiceException",
                    }
                    or isinstance(error, TimeoutError)
                    or type(error).__name__
                    in {"ReadTimeoutError", "ConnectTimeoutError", "EndpointConnectionError"}
                ):
                    raise GlueLaunchRetryable(
                        "counted Glue launch requires bounded retry"
                    ) from error
                raise
            job_run = response.get("JobRunId")
            if not isinstance(job_run, str) or not job_run or len(job_run) > 256:
                raise RuntimeConflict("Glue returned no bounded job-run identity")
            result = {
                "JobName": self.job_name,
                "JobRunId": job_run,
                "launch_authority_digest": launch_token,
            }
            self.database.update_item(
                TableName=self.table,
                Key=_key(manifest.run_id, suffix),
                ConditionExpression=(
                    "binding = :binding AND invocation_id = :owner AND #state = :reserved"
                ),
                ExpressionAttributeNames={"#state": "state"},
                ExpressionAttributeValues={
                    ":binding": {"S": binding},
                    ":owner": {"S": invocation_id},
                    ":reserved": {"S": "RESERVED"},
                    ":completed": {"S": "COMPLETED"},
                    ":json": {"S": canonical_json(result)},
                    ":digest": {"S": digest(result)},
                },
                UpdateExpression=(
                    "SET #state = :completed, result_json = :json, result_digest = :digest"
                ),
            )
            return result
        raise AdmissionDenied("durable Glue launch budget is exhausted")
