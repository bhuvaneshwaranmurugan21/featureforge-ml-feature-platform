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
    """Build rows with explicit requested and effective prediction knowledge cutoffs."""

    if dataset_as_of < 0:
        raise ValueError("dataset_as_of must be a non-negative epoch second")
    rows: list[dict[str, Any]] = []
    for label in sorted(labels, key=lambda item: item.label_id):
        prediction_cutoff = label.prediction_knowledge_time
        if prediction_cutoff is None:  # Label normalizes this in __post_init__.
            raise ValueError("prediction knowledge cutoff was not normalized")
        knowledge_cutoff = min(prediction_cutoff, dataset_as_of)
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
            "event_cutoff": label.label_time,
            "prediction_knowledge_cutoff": prediction_cutoff,
            "dataset_as_of": dataset_as_of,
            "knowledge_cutoff": knowledge_cutoff,
            "knowledge_truncated_by_dataset_as_of": dataset_as_of < prediction_cutoff,
        }
        for feature in values:
            row[feature.feature_name] = feature.value
        rows.append(row)
    definition_digests = sorted(definition.definition_digest for definition in definitions)
    rows_tuple = tuple(rows)
    row_cutoffs = [
        {
            "dataset_as_of": row["dataset_as_of"],
            "effective_knowledge_cutoff": row["knowledge_cutoff"],
            "event_cutoff": row["event_cutoff"],
            "label_id": row["label_id"],
            "prediction_knowledge_cutoff": row["prediction_knowledge_cutoff"],
            "truncated_by_dataset_as_of": row["knowledge_truncated_by_dataset_as_of"],
        }
        for row in rows
    ]
    manifest = {
        "dataset_as_of": dataset_as_of,
        "definition_digests": definition_digests,
        "generation_id": generation_id,
        "label_ids": [label.label_id for label in sorted(labels, key=lambda item: item.label_id)],
        "row_count": len(rows),
        "row_cutoffs": row_cutoffs,
        "rows_digest": digest(rows_tuple),
    }
    manifest["manifest_digest"] = digest(manifest)
    return TrainingDataset(rows_tuple, manifest)
