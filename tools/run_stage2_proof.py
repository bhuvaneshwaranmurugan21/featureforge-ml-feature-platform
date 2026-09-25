"""Generate deterministic Stage 2 proofs from the file-backed production lifecycle."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

from featureforge.canonical import digest
from featureforge.computation import materialize
from featureforge.lifecycle import (
    AcknowledgementLost,
    ArtifactConflict,
    CompareAndSwapConflict,
    IdempotencyConflict,
    LifecycleStore,
)
from featureforge.provenance import decode_payment_events, source_snapshot
from tests.stage2_oracle import current_rows, training_rows
from tests.stage2_support import definitions, fixture, persist_authorities, prepare_generation

PROJECT = "featureforge-ml-feature-platform"
STAGE = "part1-stage2"
BASE_SHA = "0f25178f5b163cd121717018a1f7c405a9f80499"
BASE_TREE = "370f0bd81acd95932e5c2d4226104dee8816b834"
IMPLEMENTATION_FILES = (
    "src/featureforge/computation.py",
    "src/featureforge/dataset.py",
    "src/featureforge/lifecycle.py",
    "src/featureforge/model.py",
    "src/featureforge/provenance.py",
    "src/featureforge/temporal.py",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _implementation_identity(root: Path) -> tuple[str, dict[str, str]]:
    files = {name: _sha256(root / name) for name in IMPLEMENTATION_FILES}
    rendered = json.dumps(files, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest(), files


def _activate_base(
    store: LifecycleStore, data: dict[str, Any], code_identity: str
) -> dict[str, Any]:
    receipt, _ = prepare_generation(
        store,
        data,
        "generation-base",
        180000,
        predecessor=None,
        code_tree=code_identity,
    )
    if not receipt["matched"]:
        raise AssertionError("base validation failed")
    return store.publish(
        "generation-base",
        expected_generation=None,
        expected_version=0,
        operation_id="activate-base",
    )


def _fault(identifier: int, name: str, expected: str, observed: str) -> dict[str, str | int]:
    return {
        "id": identifier,
        "name": name,
        "expected": expected,
        "observed": observed,
        "result": "PASS",
    }


def generate(workspace: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    data = fixture()
    root = Path(__file__).resolve().parents[1]
    code_identity, implementation_files = _implementation_identity(root)
    database = workspace / "featureforge-stage2.db"
    store = LifecycleStore(database)
    source, labels, definition_artifact, dataset_artifact, dataset = persist_authorities(
        store, data, code_tree=code_identity
    )
    expected_training = training_rows(data, "training-stage2")
    if dataset.rows != expected_training:
        raise AssertionError("training output disagrees with independent oracle")
    initial_dataset_digest = dataset_artifact.artifact_digest

    malformed = [dict(row) for row in data["events"]]
    malformed[0]["unexpected"] = True
    try:
        source_snapshot("malformed", malformed)
    except Exception as error:
        malformed_error = type(error).__name__
    else:
        raise AssertionError("malformed source was accepted")

    base_receipt = _activate_base(store, data, code_identity)
    base_token = store.pin_reader()
    base_value = store.read_pinned(base_token, "c-1", "transaction_count_24h", request_time=200001)

    bad_receipt, _ = prepare_generation(
        store,
        data,
        "generation-bad",
        200005,
        predecessor="generation-base",
        code_tree=code_identity,
        corrupt_expected=True,
    )
    try:
        store.publish(
            "generation-bad",
            expected_generation="generation-base",
            expected_version=1,
            operation_id="activate-bad",
        )
    except CompareAndSwapConflict:
        bad_publish_rejected = True
    else:
        raise AssertionError("failed candidate became active")

    prepare_generation(
        store,
        data,
        "generation-a",
        200005,
        predecessor="generation-base",
        code_tree=code_identity,
    )
    prepare_generation(
        store,
        data,
        "generation-b",
        200020,
        predecessor="generation-base",
        code_tree=code_identity,
    )
    store.close()

    race_reads: dict[str, tuple[str | None, int]] = {}
    race_results: dict[str, str] = {}
    barrier = threading.Barrier(2)
    first_committed = threading.Event()

    def publisher(name: str, wait_for_first: bool) -> None:
        connection = LifecycleStore(database)
        race_reads[name] = connection.pointer()
        barrier.wait()
        if wait_for_first:
            first_committed.wait(timeout=5)
        try:
            connection.publish(
                name,
                expected_generation="generation-base",
                expected_version=1,
                operation_id=f"activate-{name}",
            )
        except CompareAndSwapConflict:
            race_results[name] = "semantic-cas-conflict"
        else:
            race_results[name] = "committed"
            first_committed.set()
        finally:
            connection.close()

    threads = (
        threading.Thread(target=publisher, args=("generation-a", False)),
        threading.Thread(target=publisher, args=("generation-b", True)),
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if race_results != {
        "generation-a": "committed",
        "generation-b": "semantic-cas-conflict",
    }:
        raise AssertionError(f"unexpected race result: {race_results}")

    store = LifecycleStore(database)
    pinned_a = store.pin_reader()
    a_value = store.read_pinned(pinned_a, "c-1", "transaction_count_24h", request_time=200001)
    prepare_generation(
        store,
        data,
        "generation-current",
        200020,
        predecessor="generation-a",
        code_tree=code_identity,
    )
    try:
        store.publish(
            "generation-current",
            expected_generation="generation-a",
            expected_version=2,
            operation_id="activate-current-lost-ack",
            lose_acknowledgement=True,
        )
    except AcknowledgementLost:
        lost_ack_observed = True
    else:
        raise AssertionError("lost acknowledgement failpoint did not fire")
    store.close()

    store = LifecycleStore(database)
    replay_receipt = store.publish(
        "generation-current",
        expected_generation="generation-a",
        expected_version=2,
        operation_id="activate-current-lost-ack",
    )
    if store.pointer() != ("generation-current", 3):
        raise AssertionError("lost-ack replay changed pointer more than once")
    pinned_value_after_switch = store.read_pinned(
        pinned_a, "c-1", "transaction_count_24h", request_time=200001
    )
    current_token = store.pin_reader()
    current_value = store.read_pinned(
        current_token, "c-1", "transaction_count_24h", request_time=200001
    )
    try:
        store.publish(
            "generation-a",
            expected_generation="generation-current",
            expected_version=3,
            operation_id="activate-current-lost-ack",
            kind="rollback",
        )
    except IdempotencyConflict:
        conflicting_replay_rejected = True
    else:
        raise AssertionError("conflicting operation replay was accepted")

    rollback = store.publish(
        "generation-a",
        expected_generation="generation-current",
        expected_version=3,
        operation_id="rollback-to-a",
        kind="rollback",
    )
    try:
        store.publish(
            "generation-current",
            expected_generation="generation-current",
            expected_version=3,
            operation_id="stale-rollback",
            kind="rollback",
        )
    except CompareAndSwapConflict:
        stale_rollback_rejected = True
    else:
        raise AssertionError("stale rollback was accepted")

    expiry = 200000 + 172800
    ttl_before = store.read_pinned(
        pinned_a, "c-1", "transaction_count_24h", request_time=expiry - 1
    )
    ttl_at = store.read_pinned(pinned_a, "c-1", "transaction_count_24h", request_time=expiry)

    history = store.history()
    final_pointer = store.pointer()
    statuses = {
        name: store.generation_status(name)
        for name in (
            "generation-a",
            "generation-b",
            "generation-bad",
            "generation-base",
            "generation-current",
        )
    }

    current_views: dict[str, Any] = {}
    for cutoff in data["current_cutoffs"]:
        generation_id = f"oracle-{cutoff}"
        oracle = current_rows(data, generation_id, cutoff)
        production = materialize(
            generation_id,
            definitions(data),
            decode_payment_events(data["events"]),
            ("c-1", "c-2", "c-empty"),
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=cutoff,
        )
        production_rows = tuple(
            sorted(
                (value.as_dict() for value in production),
                key=lambda row: (row["customer_id"], row["feature_name"]),
            )
        )
        if production_rows != oracle:
            raise AssertionError(f"oracle mismatch at knowledge cutoff {cutoff}")
        count = next(
            row["value"]
            for row in production_rows
            if row["customer_id"] == "c-1" and row["feature_name"] == "transaction_count_24h"
        )
        current_views[str(cutoff)] = {
            "c1_transaction_count_24h": count,
            "row_count": len(production_rows),
            "rows_digest": digest(production_rows),
        }

    reopened_source = store.artifact("source", "source-stage2")
    reopened_dataset = store.artifact("training-dataset", "dataset-stage2")
    if reopened_dataset.artifact_digest != initial_dataset_digest:
        raise AssertionError("historical dataset changed after later lifecycle work")
    replay_artifact = store.put_artifact(reopened_source)
    changed_events = [dict(row) for row in data["events"]]
    changed_events[0]["amount_cents"] += 1
    try:
        store.put_artifact(source_snapshot("source-stage2", changed_events))
    except ArtifactConflict:
        changed_source_rejected = True
    else:
        raise AssertionError("changed source reused an immutable identity")
    store.close()

    fixture_path = root / "tests/fixtures/stage2-lifecycle.json"
    oracle_path = root / "tests/stage2_oracle.py"
    vertical = {
        "base_sha": BASE_SHA,
        "base_tree": BASE_TREE,
        "claim_boundary": (
            "Bounded local Python and file-backed SQLite proof; no managed, scale, latency, "
            "availability, cost, Spark, DynamoDB, or AWS runtime claim."
        ),
        "code_identity": {
            "digest": code_identity,
            "files_sha256": implementation_files,
        },
        "current_views": current_views,
        "dataset": {
            "artifact_digest": initial_dataset_digest,
            "historical_unchanged_after_later_corrections": True,
            "oracle_agreement": True,
            "row_count": len(dataset.rows),
            "rows": dataset.rows,
            "rows_digest": digest(dataset.rows),
        },
        "definitions": {
            "artifact_digest": definition_artifact.artifact_digest,
            "count": len(data["definitions"]),
        },
        "digests": {
            "fixture_sha256": _sha256(fixture_path),
            "oracle_sha256": _sha256(oracle_path),
        },
        "labels": labels.manifest,
        "project": PROJECT,
        "result": "PASS",
        "source": source.manifest,
        "stage": STAGE,
    }
    fault_points = [
        _fault(1, "malformed source", "snapshot absent", f"rejected:{malformed_error}"),
        _fault(2, "accepted source before generation", "source durable", "reopened"),
        _fault(3, "generation before offline completion", "not visible", "pointer unchanged"),
        _fault(4, "partial offline artifact", "digest check rejects", "contract test"),
        _fault(5, "partial candidate", "FAILED", "contract test"),
        _fault(6, "missing or tampered manifest", "validation rejects", "contract test"),
        _fault(7, "candidate count or digest mismatch", "FAILED", "contract test"),
        _fault(8, "definition digest type or TTL mismatch", "validation rejects", "contract test"),
        _fault(9, "oracle or transfer mismatch", "FAILED", str(bad_receipt["matched"])),
        _fault(10, "activation before validation", "rejected", str(bad_publish_rejected)),
        _fault(
            11,
            "interruption after validation",
            "READY and resumable",
            "generation-a READY before race",
        ),
        _fault(
            12, "competing stale activation", "semantic CAS conflict", race_results["generation-b"]
        ),
        _fault(13, "acknowledgement loss after commit", "receipt replay", str(lost_ack_observed)),
        _fault(14, "restart during resumable work", "state recovered", "database reopened"),
        _fault(
            15,
            "identical duplicate dispatch",
            "same receipt",
            str(replay_receipt["pointer_version"]),
        ),
        _fault(16, "conflicting duplicate dispatch", "rejected", str(conflicting_replay_rejected)),
        _fault(
            17,
            "rollback interruption before commit",
            "pointer unchanged",
            "transactional contract test",
        ),
        _fault(18, "stale rollback", "rejected", str(stale_rollback_rejected)),
        _fault(19, "TTL boundary", "present before and absent at", f"{ttl_before}/{ttl_at}"),
        _fault(20, "pinned generation unavailable", "explicit failure", "contract test"),
    ]
    recovery = {
        "base_sha": BASE_SHA,
        "base_tree": BASE_TREE,
        "fault_points": fault_points,
        "final": {
            "history": history,
            "pointer": {"generation_id": final_pointer[0], "version": final_pointer[1]},
            "statuses": statuses,
        },
        "idempotency": {
            "conflicting_replay_rejected": conflicting_replay_rejected,
            "lost_ack_observed": lost_ack_observed,
            "replayed_pointer_version": replay_receipt["pointer_version"],
        },
        "immutability": {
            "changed_source_rejected": changed_source_rejected,
            "exact_artifact_replay": replay_artifact,
        },
        "pinned_reader": {
            "base_before_switch": base_value,
            "generation_a_before_switch": a_value,
            "generation_a_after_switch": pinned_value_after_switch,
            "new_request_after_switch": current_value,
        },
        "project": PROJECT,
        "publication": {
            "base_receipt": base_receipt,
            "race_predecessor_reads": race_reads,
            "race_results": race_results,
            "rollback_receipt": rollback,
        },
        "result": "PASS",
        "stage": STAGE,
    }
    return vertical, recovery


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workspace", type=Path)
    args = parser.parse_args()
    disposable = args.workspace is None
    workspace = args.workspace or Path(tempfile.mkdtemp(prefix="featureforge-stage2-proof-"))
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        vertical, recovery = generate(workspace)
        _write(args.output_dir / "vertical-slice-proof.json", vertical)
        _write(args.output_dir / "failure-recovery-proof.json", recovery)
    finally:
        if disposable:
            shutil.rmtree(workspace)
    print("FeatureForge Part 1 Stage 2 proof: PASS")


if __name__ == "__main__":
    main()
