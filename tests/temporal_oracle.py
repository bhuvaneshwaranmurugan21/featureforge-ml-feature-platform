"""Structurally independent, test-only temporal oracle using primitive records."""

from __future__ import annotations

from fractions import Fraction
from typing import Any


class OracleConflict(ValueError):
    pass


def select(
    records: list[dict[str, Any]],
    customer_id: str,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> list[dict[str, Any]]:
    revision_content: dict[str, tuple[object, ...]] = {}
    event_customer: dict[str, str] = {}
    event_clocks: set[tuple[str, int]] = set()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        required = {
            "amount_cents",
            "customer_id",
            "event_id",
            "event_time",
            "knowledge_time",
            "merchant_risk",
            "operation",
            "revision_id",
            "status",
        }
        if set(record) != required:
            raise OracleConflict("record shape conflict")
        packed = tuple(record[key] for key in sorted(record))
        revision_id = str(record["revision_id"])
        event_id = str(record["event_id"])
        if not revision_id or not event_id or not record["customer_id"]:
            raise OracleConflict("empty identity")
        if int(record["knowledge_time"]) < int(record["event_time"]):
            raise OracleConflict("knowledge precedes event")
        operation = record["operation"]
        if operation == "retract":
            if any(record[key] is not None for key in ("amount_cents", "status", "merchant_risk")):
                raise OracleConflict("retraction carries payload")
        elif operation == "upsert":
            if (
                not isinstance(record["amount_cents"], int)
                or int(record["amount_cents"]) < 0
                or record["status"] not in {"succeeded", "failed"}
                or not isinstance(record["merchant_risk"], (int, float))
                or not 0 <= float(record["merchant_risk"]) <= 1
            ):
                raise OracleConflict("invalid upsert payload")
        else:
            raise OracleConflict("unsupported operation")
        if revision_id in revision_content:
            if revision_content[revision_id] != packed:
                raise OracleConflict("revision identity conflict")
            continue
        revision_content[revision_id] = packed
        if event_id in event_customer and event_customer[event_id] != record["customer_id"]:
            raise OracleConflict("customer identity conflict")
        event_customer[event_id] = str(record["customer_id"])
        clock = (event_id, int(record["knowledge_time"]))
        if clock in event_clocks:
            raise OracleConflict("same-clock conflict")
        event_clocks.add(clock)
        grouped.setdefault(event_id, []).append(record)

    selected: list[dict[str, Any]] = []
    for revisions in grouped.values():
        knowable = [row for row in revisions if int(row["knowledge_time"]) <= knowledge_cutoff]
        if not knowable:
            continue
        latest = max(knowable, key=lambda row: int(row["knowledge_time"]))
        if (
            latest["operation"] == "upsert"
            and latest["customer_id"] == customer_id
            and int(latest["event_time"]) <= event_cutoff
        ):
            selected.append(latest)
    return sorted(
        selected,
        key=lambda row: (int(row["event_time"]), str(row["event_id"]), str(row["revision_id"])),
    )


def values(
    definitions: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    event_cutoff: int,
) -> dict[str, int | float | None]:
    result: dict[str, int | float | None] = {}
    for definition in definitions:
        seconds = definition["window_seconds"]
        candidates = [
            row
            for row in selected
            if seconds is None or int(row["event_time"]) > event_cutoff - int(seconds)
        ]
        computation = definition["computation"]
        default = definition["empty_default"]
        if computation == "transaction_count":
            value: int | float | None = len(candidates)
        elif computation == "successful_spend_cents":
            successes = [
                int(row["amount_cents"])
                for row in candidates
                if row["status"] == "succeeded"
            ]
            value = sum(successes) if successes else default
        elif computation == "failed_payment_ratio":
            failures = len([row for row in candidates if row["status"] == "failed"])
            value = float(Fraction(failures, len(candidates))) if candidates else default
        elif computation == "hours_since_success":
            successes = [
                int(row["event_time"])
                for row in candidates
                if row["status"] == "succeeded"
            ]
            value = (event_cutoff - max(successes)) / 3600 if successes else default
        elif computation == "max_merchant_risk":
            risks = [float(row["merchant_risk"]) for row in candidates]
            value = max(risks) if risks else default
        else:
            raise OracleConflict(f"unknown computation: {computation}")
        result[str(definition["name"])] = value
    return result
