"""Independent Stage 2 oracle built only from primitive records and stdlib."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from tests.temporal_oracle import select, values


def _digest(value: Any) -> str:
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def definition_digest(definition: dict[str, Any]) -> str:
    return _digest(
        {
            "computation": definition["computation"],
            "description": definition["description"],
            "empty_default": definition["empty_default"],
            "name": definition["name"],
            "ttl_seconds": definition["ttl_seconds"],
            "value_type": definition["value_type"],
            "version": definition["version"],
            "window_seconds": definition["window_seconds"],
        }
    )


def current_rows(
    fixture: dict[str, Any], generation_id: str, knowledge_cutoff: int
) -> tuple[dict[str, Any], ...]:
    event_cutoff = int(fixture["event_cutoff"])
    rows: list[dict[str, Any]] = []
    for customer_id in ("c-1", "c-2", "c-empty"):
        chosen = select(fixture["events"], customer_id, event_cutoff, knowledge_cutoff)
        computed = values(fixture["definitions"], chosen, event_cutoff)
        for definition in fixture["definitions"]:
            rows.append(
                {
                    "customer_id": customer_id,
                    "definition_digest": definition_digest(definition),
                    "event_time": event_cutoff,
                    "feature_name": definition["name"],
                    "generation_id": generation_id,
                    "knowledge_time": knowledge_cutoff,
                    "value": computed[definition["name"]],
                }
            )
    return tuple(sorted(rows, key=lambda row: (row["customer_id"], row["feature_name"])))


def training_rows(fixture: dict[str, Any], generation_id: str) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    dataset_as_of = int(fixture["historical_knowledge_cutoff"])
    for label in sorted(fixture["labels"], key=lambda item: item["label_id"]):
        event_cutoff = int(label["label_time"])
        requested = int(label["prediction_knowledge_time"])
        effective = min(requested, dataset_as_of)
        chosen = select(fixture["events"], label["customer_id"], event_cutoff, effective)
        computed = values(fixture["definitions"], chosen, event_cutoff)
        rows.append(
            {
                "customer_id": label["customer_id"],
                "dataset_as_of": dataset_as_of,
                "event_cutoff": event_cutoff,
                **computed,
                "knowledge_cutoff": effective,
                "knowledge_truncated_by_dataset_as_of": dataset_as_of < requested,
                "label": label["value"],
                "label_id": label["label_id"],
                "label_time": event_cutoff,
                "prediction_knowledge_cutoff": requested,
            }
        )
    return tuple(rows)
