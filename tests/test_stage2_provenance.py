from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

import pytest

from featureforge.lifecycle import (
    ArtifactConflict,
    DefinitionConflict,
    LifecycleError,
    LifecycleStore,
)
from featureforge.provenance import ContractError, label_snapshot, rejection_report, source_snapshot
from tests.stage2_support import definitions, fixture, persist_authorities


def test_source_label_dataset_and_definition_artifacts_are_deterministic(tmp_path: Path) -> None:
    data = fixture()
    first = LifecycleStore(tmp_path / "first.db")
    second = LifecycleStore(tmp_path / "second.db")
    left = persist_authorities(first, data)
    right = persist_authorities(second, data)
    assert [artifact.artifact_digest for artifact in left[:4]] == [
        artifact.artifact_digest for artifact in right[:4]
    ]
    assert left[4] == right[4]
    first.close()
    second.close()


def test_snapshot_replay_is_idempotent_and_changed_content_conflicts(tmp_path: Path) -> None:
    data = fixture()
    store = LifecycleStore(tmp_path / "lifecycle.db")
    artifact = source_snapshot("source", list(reversed(data["events"])))
    assert store.put_artifact(artifact) == "written"
    assert store.put_artifact(source_snapshot("source", data["events"])) == "replayed"
    changed = copy.deepcopy(data["events"])
    changed[0]["amount_cents"] += 1
    with pytest.raises(ArtifactConflict):
        store.put_artifact(source_snapshot("source", changed))


@pytest.mark.parametrize(
    "edit",
    [
        lambda row: row.pop("status"),
        lambda row: row.update(extra="unknown"),
        lambda row: row.update(event_time=True),
        lambda row: row.update(knowledge_time=1),
        lambda row: row.update(operation="retract", amount_cents=1),
    ],
)
def test_malformed_source_is_rejected_as_one_snapshot(edit: object) -> None:
    data = fixture()
    rows = copy.deepcopy(data["events"])
    assert callable(edit)
    edit(rows[0])
    with pytest.raises((ContractError, ValueError)) as raised:
        source_snapshot("bad-source", rows)
    report = rejection_report("bad-source", rows, raised.value)
    assert report["snapshot_id"] == "bad-source"
    assert report["report_digest"]


def test_label_contract_rejects_future_prediction_knowledge() -> None:
    data = fixture()
    rows = copy.deepcopy(data["labels"])
    rows[0]["prediction_knowledge_time"] = rows[0]["label_time"] + 1
    with pytest.raises(ContractError):
        label_snapshot("bad-labels", rows)


def test_persisted_definition_authority_survives_restart(tmp_path: Path) -> None:
    data = fixture()
    database = tmp_path / "lifecycle.db"
    original = definitions(data)[0]
    store = LifecycleStore(database)
    assert store.register_definition(original) == "written"
    store.close()
    reopened = LifecycleStore(database)
    assert reopened.register_definition(original) == "replayed"
    changed = copy.deepcopy(data["definitions"][0])
    changed["ttl_seconds"] += 1
    with pytest.raises(DefinitionConflict):
        reopened.register_definition(type(original)(**changed))


def test_unsupported_schema_version_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "future.db"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA user_version = 99")
    connection.close()
    with pytest.raises(LifecycleError, match="unsupported lifecycle schema version"):
        LifecycleStore(database)
