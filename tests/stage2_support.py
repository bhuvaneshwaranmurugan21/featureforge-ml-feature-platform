from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from featureforge.canonical import digest
from featureforge.computation import materialize
from featureforge.dataset import TrainingDataset, build_training_dataset
from featureforge.lifecycle import LifecycleStore
from featureforge.model import FeatureDefinition
from featureforge.provenance import (
    Artifact,
    bind_dataset_manifest,
    decode_labels,
    decode_payment_events,
    definition_set,
    label_snapshot,
    rows_artifact,
    source_snapshot,
)
from tests.stage2_oracle import current_rows

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "tests/fixtures/stage2-lifecycle.json"


def fixture() -> dict[str, Any]:
    value: dict[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return value


def definitions(data: dict[str, Any]) -> tuple[FeatureDefinition, ...]:
    return tuple(FeatureDefinition(**row) for row in data["definitions"])


def persist_authorities(
    store: LifecycleStore, data: dict[str, Any], *, code_tree: str = "stage2-code-tree"
) -> tuple[Artifact, Artifact, Artifact, Artifact, TrainingDataset]:
    defs = definitions(data)
    for definition in defs:
        store.register_definition(definition)
    source = source_snapshot("source-stage2", data["events"])
    labels = label_snapshot("labels-stage2", data["labels"])
    definition_manifest = definition_set(defs)
    definition_artifact = rows_artifact(
        "definitions-stage2",
        "definition-set",
        "feature-definition-set-v1",
        tuple(definition_manifest["definitions"]),
        {"definition_set_digest": definition_manifest["definition_set_digest"]},
    )
    dataset = build_training_dataset(
        "training-stage2",
        decode_labels(data["labels"]),
        defs,
        decode_payment_events(data["events"]),
        dataset_as_of=data["historical_knowledge_cutoff"],
    )
    dataset_binding = bind_dataset_manifest(
        dataset.manifest,
        source=source,
        labels=labels,
        definition_set_digest=definition_artifact.artifact_digest,
        code_tree=code_tree,
    )
    dataset_artifact = rows_artifact(
        "dataset-stage2",
        "training-dataset",
        "training-dataset-manifest-v2",
        dataset.rows,
        dataset_binding,
    )
    for artifact in (source, labels, definition_artifact, dataset_artifact):
        store.put_artifact(artifact)
    return source, labels, definition_artifact, dataset_artifact, dataset


def prepare_generation(
    store: LifecycleStore,
    data: dict[str, Any],
    generation_id: str,
    knowledge_cutoff: int,
    *,
    predecessor: str | None,
    code_tree: str = "stage2-code-tree",
    corrupt_expected: bool = False,
) -> tuple[dict[str, Any], Artifact]:
    source, labels, definition_artifact, dataset_artifact, _ = persist_authorities(
        store, data, code_tree=code_tree
    )
    defs = definitions(data)
    candidate = materialize(
        generation_id,
        defs,
        decode_payment_events(data["events"]),
        ("c-1", "c-2", "c-empty"),
        event_cutoff=data["event_cutoff"],
        knowledge_cutoff=knowledge_cutoff,
    )
    candidate_rows = tuple(
        sorted(
            (value.as_dict() for value in candidate),
            key=lambda row: (row["customer_id"], row["feature_name"]),
        )
    )
    oracle_rows = list(current_rows(data, generation_id, knowledge_cutoff))
    if corrupt_expected:
        oracle_rows[0] = oracle_rows[0] | {"value": 999999}
    expected = rows_artifact(
        f"expected-{generation_id}",
        "expected-current",
        "expected-current-v1",
        tuple(oracle_rows),
        {"authority": "independent-stage2-oracle"},
    )
    store.put_artifact(expected)
    manifest: dict[str, Any] = {
        "candidate_count": len(candidate_rows),
        "candidate_digest": digest(candidate_rows),
        "code_tree": code_tree,
        "contract": "generation-manifest-v1",
        "dataset_manifest_digest": dataset_artifact.artifact_digest,
        "definition_set_digest": definition_artifact.artifact_digest,
        "event_cutoff": data["event_cutoff"],
        "expected_predecessor": predecessor,
        "generation_id": generation_id,
        "knowledge_cutoff": knowledge_cutoff,
        "label_snapshot_digest": labels.artifact_digest,
        "source_snapshot_digest": source.artifact_digest,
    }
    store.create_generation(manifest)
    store.begin_build(generation_id)
    store.put_candidate_batch(generation_id, candidate)
    store.seal_candidate(generation_id)
    return store.validate_candidate(
        generation_id, expected_artifact_id=expected.artifact_id
    ), expected
