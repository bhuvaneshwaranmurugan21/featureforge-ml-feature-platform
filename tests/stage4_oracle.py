"""Independent standard-library oracle for Stage 4 online-record envelopes.

This module deliberately imports no production FeatureForge modules.
"""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal
from typing import Any


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _decimal(value: int | float) -> str:
    if isinstance(value, bool) or (isinstance(value, float) and not math.isfinite(value)):
        raise ValueError("invalid numeric value")
    number = Decimal(str(value))
    if number == 0:
        return "0"
    rendered = format(number.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def oracle_online_records(
    definitions: list[dict[str, Any]],
    values: list[dict[str, Any]],
    *,
    feature_set: str,
    operation_id: str,
    source_digest: str,
    materialized_at: int,
) -> tuple[dict[str, Any], ...]:
    ordered = sorted(values, key=lambda row: (row["customer_id"], row["feature_name"]))
    offline_rows_digest = _digest(ordered)
    definitions_by_digest = {row["definition_digest"]: row for row in definitions}
    result: list[dict[str, Any]] = []
    for row in ordered:
        definition = definitions_by_digest[row["definition_digest"]]
        raw = row["value"]
        if raw is None:
            encoded: dict[str, Any] = {"kind": "null"}
        elif definition["value_type"] == "integer":
            encoded = {"integer": raw, "kind": "integer"}
        else:
            encoded = {"decimal": _decimal(raw), "kind": "float"}
        expires_at = int(row["event_time"]) + int(definition["ttl_seconds"])
        body = {
            "contract": "online-feature-record-v1",
            "customer_id": row["customer_id"],
            "definition_digest": row["definition_digest"],
            "event_cutoff": row["event_time"],
            "expires_at": expires_at,
            "feature_name": row["feature_name"],
            "feature_set": feature_set,
            "generation_id": row["generation_id"],
            "knowledge_cutoff": row["knowledge_time"],
            "materialized_at": materialized_at,
            "offline_rows_digest": offline_rows_digest,
            "operation_id": operation_id,
            "partition_key": (
                f"ENTITY#{row['customer_id']}#GEN#{row['generation_id']}"
            ),
            "sort_key": (
                f"FEATURE#{row['feature_name']}#DEF#{row['definition_digest']}"
            ),
            "source_digest": source_digest,
            "ttl_epoch_seconds": expires_at,
            "value": encoded,
        }
        result.append(body | {"record_digest": _digest(body)})
    return tuple(
        sorted(result, key=lambda row: (row["partition_key"], row["sort_key"]))
    )
