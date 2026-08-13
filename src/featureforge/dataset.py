"""Leakage-safe, reproducible point-in-time training dataset builder."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from featureforge.canonical import digest
from featureforge.computation import materialize
from featureforge.model import FeatureDefinition, Label, PaymentEvent


@dataclass(frozen=True)
class TrainingDataset:
    rows: tuple[dict[str, Any], ...]
    manifest: dict[str, Any]


def build_training_dataset(
    generation_id: str,
    labels: tuple[Label, ...],
    definitions: tuple[FeatureDefinition, ...],
    events: tuple[PaymentEvent, ...],
    *,
    dataset_as_of: int,
) -> TrainingDataset:
    """Join using event_time <= label_time and knowledge_time <= min(label_time, as_of)."""

    rows: list[dict[str, Any]] = []
    for label in sorted(labels, key=lambda item: item.label_id):
        knowledge_cutoff = min(label.label_time, dataset_as_of)
        values = materialize(
            generation_id,
            definitions,
            events,
            (label.customer_id,),
            event_cutoff=label.label_time,
            knowledge_cutoff=knowledge_cutoff,
        )
        row: dict[str, Any] = {
            "customer_id": label.customer_id,
            "label": label.value,
            "label_id": label.label_id,
            "label_time": label.label_time,
            "knowledge_cutoff": knowledge_cutoff,
        }
        for feature in values:
            row[feature.feature_name] = feature.value
        rows.append(row)
    definition_digests = sorted(definition.definition_digest for definition in definitions)
    rows_tuple = tuple(rows)
    manifest = {
        "dataset_as_of": dataset_as_of,
        "definition_digests": definition_digests,
        "generation_id": generation_id,
        "label_ids": [label.label_id for label in sorted(labels, key=lambda item: item.label_id)],
        "row_count": len(rows),
        "rows_digest": digest(rows_tuple),
    }
    manifest["manifest_digest"] = digest(manifest)
    return TrainingDataset(rows_tuple, manifest)
