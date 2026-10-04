"""Persistent local fault proofs for launch admission, not live Glue evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.session import get_session
from botocore.validate import validate_parameters

from featureforge.aws_runtime import RuntimeConflict
from featureforge.glue_launch import DurableGlueLauncher, GlueLaunchRetryable, launch_request
from featureforge.managed import AdmissionDenied, ManagedContractError, ManagedRunManifest
from featureforge.online import validate_dynamodb_request
from tests.test_stage6_managed import manifest as base_manifest


def manifest() -> ManagedRunManifest:
    return replace(
        base_manifest(),
        account_fingerprint=hashlib.sha256(b"123456789012").hexdigest(),
        manifest_digest="",
    )


def conflict(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "LocalBoundary")


class PersistentBoundary:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lose_reservation_ack = False
        self.lose_completion_ack = False
        with self.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS items (pk TEXT, sk TEXT, body TEXT, PRIMARY KEY(pk,sk))"
            )

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    @staticmethod
    def key(item: dict[str, Any]) -> tuple[str, str]:
        return item["PK"]["S"], item["SK"]["S"]

    def get_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("GetItem", request)
        assert request["ConsistentRead"] is True
        with self.connect() as db:
            row = db.execute(
                "SELECT body FROM items WHERE pk=? AND sk=?", self.key(request["Key"])
            ).fetchone()
        return {} if row is None else {"Item": json.loads(row[0])}

    def scan(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("Scan", request)
        with self.connect() as db:
            rows = db.execute("SELECT body FROM items ORDER BY pk,sk").fetchall()
        return {"Items": [json.loads(row[0]) for row in rows]}

    def put_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("PutItem", request)
        with self.connect() as db:
            try:
                db.execute(
                    "INSERT INTO items VALUES (?,?,?)",
                    (*self.key(request["Item"]), json.dumps(request["Item"])),
                )
            except sqlite3.IntegrityError as error:
                raise conflict("ConditionalCheckFailedException") from error
        return {}

    def transact_write_items(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("TransactWriteItems", request)
        check, put = request["TransactItems"]
        authority = check["ConditionCheck"]
        item = put["Put"]["Item"]
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            raw = db.execute(
                "SELECT body FROM items WHERE pk=? AND sk=?", self.key(authority["Key"])
            ).fetchone()
            if (
                raw is None
                or json.loads(raw[0])["binding"]
                != authority["ExpressionAttributeValues"][":binding"]
            ):
                raise conflict("TransactionCanceledException")
            try:
                db.execute("INSERT INTO items VALUES (?,?,?)", (*self.key(item), json.dumps(item)))
            except sqlite3.IntegrityError as error:
                raise conflict("TransactionCanceledException") from error
        if self.lose_reservation_ack:
            self.lose_reservation_ack = False
            raise TimeoutError("reservation committed, acknowledgement lost")
        return {}

    def update_item(self, **request: Any) -> dict[str, Any]:
        validate_dynamodb_request("UpdateItem", request)
        values = request["ExpressionAttributeValues"]
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT body FROM items WHERE pk=? AND sk=?", self.key(request["Key"])
            ).fetchone()
            assert row is not None
            item = json.loads(row[0])
            if (
                item["binding"] != values[":binding"]
                or item["invocation_id"] != values[":owner"]
                or item["state"] != values[":reserved"]
            ):
                raise conflict("ConditionalCheckFailedException")
            item.update(
                state=values[":completed"],
                result_json=values[":json"],
                result_digest=values[":digest"],
            )
            db.execute(
                "UPDATE items SET body=? WHERE pk=? AND sk=?", (json.dumps(item), *self.key(item))
            )
        if self.lose_completion_ack:
            self.lose_completion_ack = False
            raise TimeoutError("completion committed, acknowledgement lost")
        return {}


class GlueBoundary:
    def __init__(self, *, lose_ack: bool = False, attempts: int = 1) -> None:
        self.meta = SimpleNamespace(config=Config(retries={"total_max_attempts": attempts}))
        self.calls: list[dict[str, Any]] = []
        self.lose_ack = lose_ack
        self.lock = threading.Lock()

    def get_job(self, **request: Any) -> dict[str, Any]:
        return {
            "Job": {
                "Name": request["JobName"],
                "MaxRetries": 0,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
            }
        }

    def start_job_run(self, **request: Any) -> dict[str, Any]:
        validate_parameters(
            request,
            get_session().get_service_model("glue").operation_model("StartJobRun").input_shape,
        )
        with self.lock:
            self.calls.append(request)
            run_id = f"jr-{len(self.calls)}"
        if self.lose_ack:
            raise TimeoutError("Glue accepted the run, acknowledgement lost")
        return {"JobRunId": run_id}


def launcher(db: PersistentBoundary, glue: GlueBoundary) -> DurableGlueLauncher:
    return DurableGlueLauncher(
        db,
        glue,
        table="control",
        job_name="featureforge-stage6-run-001-offline",
        kms_key="key",
        account="123456789012",
    )


def test_completed_launch_replays_after_restart_without_another_call(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite"
    glue = GlueBoundary()
    first = launcher(PersistentBoundary(path), glue).start(
        manifest(), execution_id="execution-1", invocation_id="call-1"
    )
    second = launcher(PersistentBoundary(path), glue).start(
        manifest(), execution_id="execution-1", invocation_id="call-2"
    )
    assert first == second
    assert first["JobName"] == "featureforge-stage6-run-001-offline"
    assert first["JobRunId"] == "jr-1"
    assert len(first["launch_authority_digest"]) == 64
    assert len(glue.calls) == 1


def test_unknown_glue_results_consume_exactly_three_durable_slots(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite"
    glue = GlueBoundary(lose_ack=True)
    for number in range(3):
        with pytest.raises(GlueLaunchRetryable):
            launcher(PersistentBoundary(path), glue).start(
                manifest(), execution_id="execution-1", invocation_id=f"call-{number}"
            )
    for number in range(3, 12):
        with pytest.raises(AdmissionDenied, match="budget is exhausted"):
            launcher(PersistentBoundary(path), glue).start(
                manifest(), execution_id="execution-1", invocation_id=f"call-{number}"
            )
    assert len(glue.calls) == 3


def test_reservation_and_completion_acknowledgement_loss(tmp_path: Path) -> None:
    db = PersistentBoundary(tmp_path / "control.sqlite")
    glue = GlueBoundary()
    db.lose_reservation_ack = True
    with pytest.raises(TimeoutError):
        launcher(db, glue).start(manifest(), execution_id="execution-1", invocation_id="call-1")
    assert not glue.calls
    db.lose_completion_ack = True
    with pytest.raises(TimeoutError):
        launcher(db, glue).start(manifest(), execution_id="execution-1", invocation_id="call-2")
    result = launcher(db, glue).start(
        manifest(), execution_id="execution-1", invocation_id="call-3"
    )
    assert result["JobRunId"] == "jr-1"
    assert len(glue.calls) == 1


def test_parallel_connections_cannot_exceed_physical_call_budget(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite"
    glue = GlueBoundary(lose_ack=True)
    PersistentBoundary(path)

    def invoke(number: int) -> None:
        with pytest.raises((GlueLaunchRetryable, AdmissionDenied)):
            launcher(PersistentBoundary(path), glue).start(
                manifest(), execution_id="execution-1", invocation_id=f"call-{number}"
            )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(invoke, range(24)))
    assert len(glue.calls) == 3


def test_source_or_execution_change_cannot_reset_budget(tmp_path: Path) -> None:
    db = PersistentBoundary(tmp_path / "control.sqlite")
    glue = GlueBoundary()
    launch = launcher(db, glue)
    launch.start(manifest(), execution_id="execution-1", invocation_id="call-1")
    with pytest.raises(RuntimeConflict):
        launch.start(manifest(), execution_id="execution-2", invocation_id="call-2")
    with pytest.raises(RuntimeConflict):
        launch.start(
            replace(manifest(), source_commit="a" * 40, manifest_digest=""),
            execution_id="execution-1",
            invocation_id="call-3",
        )
    assert len(glue.calls) == 1


def test_sdk_hidden_retries_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManagedContractError, match="one physical attempt"):
        launcher(PersistentBoundary(tmp_path / "control.sqlite"), GlueBoundary(attempts=2))


def test_launch_request_binds_inputs_outputs_and_compute_limits() -> None:
    request = launch_request(
        manifest(),
        job_name="featureforge-stage6-run-001-offline",
        kms_key="key",
        account="123456789012",
    )
    assert "MaxRetries" not in request  # The actual GetJob response owns this setting.
    assert request["Timeout"] == 15
    assert request["NumberOfWorkers"] == 2
    assert request["JobRunQueuingEnabled"] is False
    assert request["Arguments"]["--input-version"] == "v1"
    assert request["Arguments"]["--output-prefix"] == manifest().output_prefix


def test_service_job_retries_reject_before_reservation(tmp_path: Path) -> None:
    class RetryingJob(GlueBoundary):
        def get_job(self, **request: Any) -> dict[str, Any]:
            return {
                "Job": {
                    "Name": request["JobName"],
                    "MaxRetries": 1,
                    "ExecutionProperty": {"MaxConcurrentRuns": 1},
                }
            }

    db = PersistentBoundary(tmp_path / "control.sqlite")
    glue = RetryingJob()
    with pytest.raises(AdmissionDenied, match="deployed Glue job retries"):
        launcher(db, glue).start(manifest(), execution_id="execution-1", invocation_id="call-1")
    assert not glue.calls
    with db.connect() as connection:
        assert connection.execute("SELECT count(*) FROM items").fetchone()[0] == 0
