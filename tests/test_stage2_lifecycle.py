from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from featureforge.canonical import canonical_json, digest
from featureforge.computation import materialize
from featureforge.lifecycle import (
    AcknowledgementLost,
    ArtifactConflict,
    CompareAndSwapConflict,
    IdempotencyConflict,
    LifecycleStore,
    PinnedGenerationUnavailable,
    ReaderToken,
    StateConflict,
)
from featureforge.model import FeatureValue
from featureforge.provenance import decode_payment_events, rows_artifact
from tests.stage2_support import definitions, fixture, persist_authorities, prepare_generation


def _activate_base(store: LifecycleStore, data: dict[str, object]) -> dict[str, object]:
    receipt, _ = prepare_generation(store, data, "generation-base", 180000, predecessor=None)
    assert receipt["matched"] is True
    return store.publish(
        "generation-base",
        expected_generation=None,
        expected_version=0,
        operation_id="activate-base",
    )


def test_candidate_isolation_validation_and_pinned_reader(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    _activate_base(store, data)
    base_token = store.pin_reader()
    assert base_token.generation_id == "generation-base"
    assert store.read_pinned(base_token, "c-1", "transaction_count_24h", request_time=200001) == 3

    receipt, _ = prepare_generation(
        store, data, "generation-next", 200005, predecessor="generation-base"
    )
    assert receipt["matched"] is True
    assert store.pointer() == ("generation-base", 1)
    assert store.read_pinned(base_token, "c-1", "transaction_count_24h", request_time=200001) == 3
    store.publish(
        "generation-next",
        expected_generation="generation-base",
        expected_version=1,
        operation_id="activate-next",
    )
    next_token = store.pin_reader()
    assert next_token.generation_id == "generation-next"
    assert store.read_pinned(base_token, "c-1", "transaction_count_24h", request_time=200001) == 3
    assert store.read_pinned(next_token, "c-1", "transaction_count_24h", request_time=200001) == 4


def test_ttl_comes_from_persisted_definition_at_exact_boundary(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    _activate_base(store, data)
    token = store.pin_reader()
    expiry = 200000 + 172800
    assert store.read_pinned(token, "c-1", "transaction_count_24h", request_time=expiry - 1) == 3
    assert store.read_pinned(token, "c-1", "transaction_count_24h", request_time=expiry) is None
    assert store.read_pinned(token, "c-1", "transaction_count_24h", request_time=expiry + 1) is None


def test_failed_validation_and_unvalidated_candidate_cannot_publish(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    _activate_base(store, data)
    receipt, _ = prepare_generation(
        store,
        data,
        "generation-bad",
        200005,
        predecessor="generation-base",
        corrupt_expected=True,
    )
    assert receipt["matched"] is False
    assert store.generation_status("generation-bad") == "FAILED"
    with pytest.raises(CompareAndSwapConflict):
        store.publish(
            "generation-bad",
            expected_generation="generation-base",
            expected_version=1,
            operation_id="bad-publish",
        )
    assert store.pointer() == ("generation-base", 1)

    source, labels, definition_artifact, dataset_artifact, _ = persist_authorities(store, data)
    rows = tuple()
    manifest = {
        "candidate_count": 0,
        "candidate_digest": digest(rows),
        "code_tree": "stage2-code-tree",
        "contract": "generation-manifest-v1",
        "dataset_manifest_digest": dataset_artifact.artifact_digest,
        "definition_set_digest": definition_artifact.artifact_digest,
        "event_cutoff": data["event_cutoff"],
        "expected_predecessor": "generation-base",
        "generation_id": "generation-unvalidated",
        "knowledge_cutoff": 200005,
        "label_snapshot_digest": labels.artifact_digest,
        "source_snapshot_digest": source.artifact_digest,
    }
    store.create_generation(manifest)
    with pytest.raises(CompareAndSwapConflict):
        store.publish(
            "generation-unvalidated",
            expected_generation="generation-base",
            expected_version=1,
            operation_id="premature",
        )


def test_partial_candidate_fails_manifest_and_expected_set_validation(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    source, labels, definition_artifact, dataset_artifact, _ = persist_authorities(store, data)
    values = materialize(
        "generation-partial",
        definitions(data),
        decode_payment_events(data["events"]),
        ("c-1", "c-2", "c-empty"),
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=200005,
    )
    full_rows = tuple(
        sorted(
            (v.as_dict() for v in values), key=lambda row: (row["customer_id"], row["feature_name"])
        )
    )
    expected = rows_artifact(
        "expected-partial",
        "expected-current",
        "expected-current-v1",
        full_rows,
        {"authority": "test"},
    )
    store.put_artifact(expected)
    manifest = {
        "candidate_count": len(full_rows),
        "candidate_digest": digest(full_rows),
        "code_tree": "stage2-code-tree",
        "contract": "generation-manifest-v1",
        "dataset_manifest_digest": dataset_artifact.artifact_digest,
        "definition_set_digest": definition_artifact.artifact_digest,
        "event_cutoff": data["event_cutoff"],
        "expected_predecessor": None,
        "generation_id": "generation-partial",
        "knowledge_cutoff": 200005,
        "label_snapshot_digest": labels.artifact_digest,
        "source_snapshot_digest": source.artifact_digest,
    }
    store.create_generation(manifest)
    store.begin_build("generation-partial")
    store.put_candidate_batch("generation-partial", values[:3])
    store.seal_candidate("generation-partial")
    receipt = store.validate_candidate(
        "generation-partial", expected_artifact_id="expected-partial"
    )
    assert receipt["matched"] is False
    assert receipt["candidate_matches_manifest"] is False


def test_post_validation_tamper_is_detected_before_cas(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    _activate_base(store, data)
    prepare_generation(store, data, "generation-next", 200005, predecessor="generation-base")
    row = store.connection.execute(
        "SELECT row_json FROM candidate_values WHERE generation_id=? LIMIT 1",
        ("generation-next",),
    ).fetchone()
    changed = json.loads(row["row_json"])
    changed["value"] = 123456
    store.connection.execute(
        """UPDATE candidate_values SET row_json=?
           WHERE generation_id=? AND customer_id=? AND feature_name=?""",
        (
            canonical_json(changed),
            "generation-next",
            changed["customer_id"],
            changed["feature_name"],
        ),
    )
    with pytest.raises(CompareAndSwapConflict, match="changed after validation"):
        store.publish(
            "generation-next",
            expected_generation="generation-base",
            expected_version=1,
            operation_id="tampered",
        )
    assert store.pointer() == ("generation-base", 1)


def test_two_independent_publishers_have_one_semantic_cas_winner(tmp_path: Path) -> None:
    data = fixture()
    database = tmp_path / "lifecycle.db"
    setup = LifecycleStore(database)
    _activate_base(setup, data)
    prepare_generation(setup, data, "generation-a", 200005, predecessor="generation-base")
    prepare_generation(setup, data, "generation-b", 200020, predecessor="generation-base")
    setup.close()
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, str]] = []

    def publish(generation_id: str) -> None:
        connection = LifecycleStore(database)
        barrier.wait()
        try:
            connection.publish(
                generation_id,
                expected_generation="generation-base",
                expected_version=1,
                operation_id=f"activate-{generation_id}",
            )
        except CompareAndSwapConflict:
            outcomes.append((generation_id, "semantic-cas-conflict"))
        else:
            outcomes.append((generation_id, "committed"))
        finally:
            connection.close()

    threads = [
        threading.Thread(target=publish, args=(name,)) for name in ("generation-a", "generation-b")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcome for _, outcome in outcomes) == ["committed", "semantic-cas-conflict"]
    verified = LifecycleStore(database)
    assert verified.pointer()[1] == 2
    assert len(verified.history()) == 2


def test_lost_ack_restart_idempotency_rollback_and_stale_rollback(tmp_path: Path) -> None:
    data = fixture()
    database = tmp_path / "lifecycle.db"
    store = LifecycleStore(database)
    _activate_base(store, data)
    prepare_generation(store, data, "generation-next", 200005, predecessor="generation-base")
    with pytest.raises(AcknowledgementLost):
        store.publish(
            "generation-next",
            expected_generation="generation-base",
            expected_version=1,
            operation_id="lost-ack",
            lose_acknowledgement=True,
        )
    store.close()
    reopened = LifecycleStore(database)
    replay = reopened.publish(
        "generation-next",
        expected_generation="generation-base",
        expected_version=1,
        operation_id="lost-ack",
    )
    assert replay["pointer_version"] == 2
    assert reopened.pointer() == ("generation-next", 2)
    with pytest.raises(IdempotencyConflict):
        reopened.publish(
            "generation-base",
            expected_generation="generation-next",
            expected_version=2,
            operation_id="lost-ack",
            kind="rollback",
        )
    rollback = reopened.publish(
        "generation-base",
        expected_generation="generation-next",
        expected_version=2,
        operation_id="rollback-base",
        kind="rollback",
    )
    assert rollback["kind"] == "rollback"
    assert reopened.pointer() == ("generation-base", 3)
    with pytest.raises(CompareAndSwapConflict):
        reopened.publish(
            "generation-next",
            expected_generation="generation-next",
            expected_version=2,
            operation_id="stale-rollback",
            kind="rollback",
        )
    assert reopened.pointer() == ("generation-base", 3)


def test_illegal_transition_fails_closed(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    source, labels, definition_artifact, dataset_artifact, _ = persist_authorities(store, data)
    manifest = {
        "candidate_count": 0,
        "candidate_digest": digest(tuple()),
        "code_tree": "stage2-code-tree",
        "contract": "generation-manifest-v1",
        "dataset_manifest_digest": dataset_artifact.artifact_digest,
        "definition_set_digest": definition_artifact.artifact_digest,
        "event_cutoff": 200000,
        "expected_predecessor": None,
        "generation_id": "generation-illegal",
        "knowledge_cutoff": 180000,
        "label_snapshot_digest": labels.artifact_digest,
        "source_snapshot_digest": source.artifact_digest,
    }
    store.create_generation(manifest)
    with pytest.raises(StateConflict):
        store.seal_candidate("generation-illegal")


def test_artifact_tamper_and_unavailable_pin_fail_closed(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    persist_authorities(store, data)
    store.connection.execute(
        """DELETE FROM artifact_rows
           WHERE kind='source' AND artifact_id='source-stage2' AND row_number=0"""
    )
    with pytest.raises(ArtifactConflict, match="do not match"):
        store.artifact("source", "source-stage2")
    with pytest.raises(PinnedGenerationUnavailable):
        store.read_pinned(
            ReaderToken("missing-generation", 999),
            "c-1",
            "transaction_count_24h",
            request_time=200001,
        )


def test_candidate_rejects_unknown_definition_and_changed_replay(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    source, labels, definition_artifact, dataset_artifact, _ = persist_authorities(store, data)
    valid = materialize(
        "generation-replay",
        definitions(data),
        decode_payment_events(data["events"]),
        ("c-1",),
        event_cutoff=200000,
        knowledge_cutoff=180000,
    )
    rows = tuple(
        sorted(
            (value.as_dict() for value in valid),
            key=lambda row: (row["customer_id"], row["feature_name"]),
        )
    )
    manifest = {
        "candidate_count": len(rows),
        "candidate_digest": digest(rows),
        "code_tree": "stage2-code-tree",
        "contract": "generation-manifest-v1",
        "dataset_manifest_digest": dataset_artifact.artifact_digest,
        "definition_set_digest": definition_artifact.artifact_digest,
        "event_cutoff": 200000,
        "expected_predecessor": None,
        "generation_id": "generation-replay",
        "knowledge_cutoff": 180000,
        "label_snapshot_digest": labels.artifact_digest,
        "source_snapshot_digest": source.artifact_digest,
    }
    store.create_generation(manifest)
    store.begin_build("generation-replay")
    assert store.put_candidate_batch("generation-replay", valid) == "written"
    assert store.put_candidate_batch("generation-replay", valid) == "replayed"
    changed = FeatureValue(
        customer_id=valid[0].customer_id,
        feature_name=valid[0].feature_name,
        value=999,
        event_time=valid[0].event_time,
        knowledge_time=valid[0].knowledge_time,
        definition_digest=valid[0].definition_digest,
        generation_id="generation-replay",
    )
    with pytest.raises(ArtifactConflict, match="different content"):
        store.put_candidate_batch("generation-replay", (changed,))
    unknown = FeatureValue(
        customer_id="c-1",
        feature_name="unknown",
        value=1,
        event_time=200000,
        knowledge_time=180000,
        definition_digest="0" * 64,
        generation_id="generation-replay",
    )
    with pytest.raises(ArtifactConflict, match="unknown definition"):
        store.put_candidate_batch("generation-replay", (unknown,))
