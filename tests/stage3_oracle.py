"""Independent standard-library oracle for Stage 3 differential proofs.

This module deliberately imports no production computation or temporal-selection code.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def oracle_rows(
    definitions: Sequence[dict[str, Any]],
    events: Sequence[dict[str, Any]],
    customer_ids: Sequence[str],
    generation_id: str,
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[dict[str, Any], ...]:
    revisions: dict[str, dict[str, Any]] = {}
    revision_ids: dict[str, dict[str, Any]] = {}
    customers: dict[str, str] = {}
    clocks: dict[tuple[str, int], str] = {}
    for event in events:
        revision_id = str(event["revision_id"])
        if revision_id in revision_ids:
            if revision_ids[revision_id] != event:
                raise ValueError("revision identity conflict")
            continue
        event_id = str(event["event_id"])
        customer_id = str(event["customer_id"])
        if customers.setdefault(event_id, customer_id) != customer_id:
            raise ValueError("customer identity drift")
        clock = (event_id, int(event["knowledge_time"]))
        if clock in clocks and clocks[clock] != revision_id:
            raise ValueError("ambiguous revision clock")
        clocks[clock] = revision_id
        revision_ids[revision_id] = event
    for event in sorted(
        revision_ids.values(),
        key=lambda row: (row["event_id"], row["knowledge_time"], row["revision_id"]),
    ):
        if int(event["knowledge_time"]) <= knowledge_cutoff:
            revisions[str(event["event_id"])] = event

    result: list[dict[str, Any]] = []
    for customer_id in sorted(customer_ids):
        current = [
            event
            for event in revisions.values()
            if event["operation"] == "upsert"
            and event["customer_id"] == customer_id
            and int(event["event_time"]) <= event_cutoff
        ]
        for definition in sorted(definitions, key=lambda row: row["name"]):
            window = definition["window_seconds"]
            candidates = [
                event
                for event in current
                if window is None or int(event["event_time"]) > event_cutoff - int(window)
            ]
            computation = definition["computation"]
            if computation == "transaction_count":
                value: int | float | None = len(candidates)
            elif computation == "successful_spend_cents":
                amounts = [
                    int(event["amount_cents"])
                    for event in candidates
                    if event["status"] == "succeeded"
                ]
                value = sum(amounts) if amounts else definition["empty_default"]
            elif computation == "failed_payment_ratio":
                value = (
                    sum(event["status"] == "failed" for event in candidates) / len(candidates)
                    if candidates
                    else definition["empty_default"]
                )
            elif computation == "hours_since_success":
                successes = [
                    int(event["event_time"])
                    for event in candidates
                    if event["status"] == "succeeded"
                ]
                value = (
                    (event_cutoff - max(successes)) / 3600
                    if successes
                    else definition["empty_default"]
                )
            elif computation == "max_merchant_risk":
                risks = [float(event["merchant_risk"]) for event in candidates]
                value = max(risks) if risks else definition["empty_default"]
            else:
                raise ValueError(f"unsupported oracle computation: {computation}")
            result.append(
                {
                    "customer_id": customer_id,
                    "feature_name": definition["name"],
                    "value": value,
                    "event_time": event_cutoff,
                    "knowledge_time": knowledge_cutoff,
                    "definition_digest": definition["definition_digest"],
                    "generation_id": generation_id,
                }
            )
    return tuple(result)
