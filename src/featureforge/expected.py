"""Independent expected-state projector for Stage 5 parity decisions.

This module deliberately uses only the Python standard library.  It does not
import the production temporal, computation, Spark, online, or parity paths.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any


class ExpectedStateError(ValueError):
    """Primitive inputs cannot produce one unambiguous expected state."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _decimal(value: int | float) -> str:
    if isinstance(value, bool) or (isinstance(value, float) and not math.isfinite(value)):
        raise ExpectedStateError("feature value is not a finite number")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ExpectedStateError("feature value is not finite")
    if number == 0:
        return "0"
    rendered = format(number.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def project_feature_rows(
    definitions: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    customer_ids: Sequence[str],
    generation_id: str,
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[dict[str, Any], ...]:
    """Reconstruct feature rows directly from primitive versioned records."""

    if not generation_id or event_cutoff < 0 or knowledge_cutoff < 0:
        raise ExpectedStateError("generation and non-negative cutoffs are required")
    revisions_by_id: dict[str, dict[str, Any]] = {}
    customers_by_event: dict[str, str] = {}
    revision_clocks: dict[tuple[str, int], str] = {}
    for raw in events:
        event = dict(raw)
        revision_id = str(event["revision_id"])
        if revision_id in revisions_by_id:
            if revisions_by_id[revision_id] != event:
                raise ExpectedStateError("revision identity conflict")
            continue
        event_id = str(event["event_id"])
        customer_id = str(event["customer_id"])
        if customers_by_event.setdefault(event_id, customer_id) != customer_id:
            raise ExpectedStateError("customer identity drift")
        clock = (event_id, int(event["knowledge_time"]))
        if clock in revision_clocks and revision_clocks[clock] != revision_id:
            raise ExpectedStateError("ambiguous revision clock")
        revision_clocks[clock] = revision_id
        revisions_by_id[revision_id] = event

    current: dict[str, dict[str, Any]] = {}
    for event in sorted(
        revisions_by_id.values(),
        key=lambda row: (str(row["event_id"]), int(row["knowledge_time"]), str(row["revision_id"])),
    ):
        if int(event["knowledge_time"]) <= knowledge_cutoff:
            current[str(event["event_id"])] = event

    result: list[dict[str, Any]] = []
    for customer_id in sorted(set(customer_ids)):
        visible = [
            event
            for event in current.values()
            if event["operation"] == "upsert"
            and event["customer_id"] == customer_id
            and int(event["event_time"]) <= event_cutoff
        ]
        for raw_definition in sorted(definitions, key=lambda row: str(row["name"])):
            definition = dict(raw_definition)
            window = definition["window_seconds"]
            candidates = [
                event
                for event in visible
                if window is None or int(event["event_time"]) > event_cutoff - int(window)
            ]
            computation = str(definition["computation"])
            value: int | float | None
            if computation == "transaction_count":
                value = len(candidates)
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
                raise ExpectedStateError(f"unsupported computation: {computation}")
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


def project_online_records(
    definitions: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
    *,
    feature_set: str,
    operation_id: str,
    source_digest: str,
    materialized_at: int,
) -> tuple[dict[str, Any], ...]:
    """Build expected online envelopes without production serialization code."""

    if not feature_set or not operation_id or not source_digest or materialized_at < 0:
        raise ExpectedStateError("online projection identities are invalid")
    ordered = sorted(
        (dict(row) for row in feature_rows),
        key=lambda row: (str(row["customer_id"]), str(row["feature_name"])),
    )
    if not ordered:
        raise ExpectedStateError("expected online state cannot be empty")
    if len({(row["customer_id"], row["feature_name"]) for row in ordered}) != len(ordered):
        raise ExpectedStateError("duplicate entity-feature key")
    offline_rows_digest = _digest(ordered)
    definitions_by_digest = {
        str(row["definition_digest"]): dict(row) for row in definitions
    }
    result: list[dict[str, Any]] = []
    for row in ordered:
        try:
            definition = definitions_by_digest[str(row["definition_digest"])]
        except KeyError as error:
            raise ExpectedStateError("feature row references an unknown definition") from error
        raw = row["value"]
        if raw is None:
            encoded: dict[str, Any] = {"kind": "null"}
        elif definition["value_type"] == "integer":
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise ExpectedStateError("integer definition received a non-integer value")
            encoded = {"integer": raw, "kind": "integer"}
        else:
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ExpectedStateError("float definition received a non-numeric value")
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
            "partition_key": f"ENTITY#{row['customer_id']}#GEN#{row['generation_id']}",
            "sort_key": f"FEATURE#{row['feature_name']}#DEF#{row['definition_digest']}",
            "source_digest": source_digest,
            "ttl_epoch_seconds": expires_at,
            "value": encoded,
        }
        result.append(body | {"record_digest": _digest(body)})
    return tuple(sorted(result, key=lambda row: (row["partition_key"], row["sort_key"])))


def deterministic_stratified_sample(
    records: Sequence[Mapping[str, Any]],
    strata_by_customer: Mapping[str, str],
    *,
    seed: int,
    per_stratum: int,
) -> tuple[tuple[str, str], ...]:
    """Select stable entity-feature keys independently inside each declared stratum."""

    if per_stratum < 1:
        raise ExpectedStateError("per-stratum sample size must be positive")
    grouped: dict[str, list[tuple[str, str]]] = {}
    for row in records:
        customer_id = str(row["customer_id"])
        feature_name = str(row["feature_name"])
        stratum = strata_by_customer.get(customer_id, "ordinary")
        grouped.setdefault(stratum, []).append((customer_id, feature_name))
    selected: list[tuple[str, str]] = []
    for stratum, keys in sorted(grouped.items()):
        ranked = sorted(
            set(keys),
            key=lambda key: hashlib.sha256(
                f"{seed}|{stratum}|{key[0]}|{key[1]}".encode()
            ).hexdigest(),
        )
        selected.extend(ranked[:per_stratum])
    return tuple(sorted(selected))


def compare_online_records(
    expected: Sequence[Mapping[str, Any]],
    actual: Sequence[Mapping[str, Any]],
    *,
    sample_keys: Sequence[tuple[str, str]] | None = None,
    request_time: int | None = None,
    maximum_freshness_age: int | None = None,
) -> dict[str, Any]:
    """Compare semantic values, integrity envelopes, membership, and eligibility."""

    expected_rows = [dict(row) for row in expected]
    actual_rows = [dict(row) for row in actual]
    expected_map = {
        (str(row["customer_id"]), str(row["feature_name"])): row for row in expected_rows
    }
    actual_map = {
        (str(row["customer_id"]), str(row["feature_name"])): row for row in actual_rows
    }
    if len(expected_map) != len(expected_rows) or len(actual_map) != len(actual_rows):
        raise ExpectedStateError("comparison input contains duplicate keys")
    all_keys = set(expected_map) | set(actual_map)
    selected = set(sample_keys) if sample_keys is not None else all_keys
    unknown = selected - all_keys
    if unknown:
        raise ExpectedStateError("sample contains unknown keys")
    missing = sorted(key for key in selected if key in expected_map and key not in actual_map)
    extra = sorted(key for key in selected if key in actual_map and key not in expected_map)
    semantic_fields = (
        "customer_id",
        "feature_name",
        "value",
        "definition_digest",
        "event_cutoff",
        "knowledge_cutoff",
        "generation_id",
        "source_digest",
        "expires_at",
    )
    semantic: list[dict[str, Any]] = []
    envelope: list[tuple[str, str]] = []
    eligibility: list[dict[str, Any]] = []
    for key in sorted(selected & set(expected_map) & set(actual_map)):
        wanted = expected_map[key]
        observed = actual_map[key]
        differing = [field for field in semantic_fields if wanted.get(field) != observed.get(field)]
        if differing:
            semantic.append({"fields": differing, "key": list(key)})
        if wanted != observed:
            envelope.append(key)
        if request_time is not None:
            if request_time >= int(observed["expires_at"]):
                eligibility.append({"key": list(key), "reason": "EXPIRED"})
            elif (
                maximum_freshness_age is not None
                and request_time - int(observed["knowledge_cutoff"]) > maximum_freshness_age
            ):
                eligibility.append({"key": list(key), "reason": "STALE"})
    aggregate_mismatch = _digest(expected_rows) != _digest(actual_rows)
    reasons: list[str] = []
    if missing:
        reasons.append("MISSING_RECORD")
    if extra:
        reasons.append("EXTRA_RECORD")
    if semantic:
        reasons.append("SEMANTIC_MISMATCH")
    if envelope:
        reasons.append("ENVELOPE_MISMATCH")
    if eligibility:
        reasons.append("INELIGIBLE_RECORD")
    if aggregate_mismatch:
        reasons.append("AGGREGATE_MISMATCH")
    body: dict[str, Any] = {
        "actual_count": len(actual_rows),
        "actual_digest": _digest(actual_rows),
        "aggregate_mismatch": aggregate_mismatch,
        "compared_count": len(selected),
        "contract": "stage5-parity-report-v1",
        "eligibility": eligibility,
        "envelope_mismatch_count": len(envelope),
        "expected_count": len(expected_rows),
        "expected_digest": _digest(expected_rows),
        "extra": [list(key) for key in extra],
        "missing": [list(key) for key in missing],
        "passed": not reasons,
        "reasons": reasons,
        "sampled": sample_keys is not None,
        "selected_keys_digest": _digest([list(key) for key in sorted(selected)]),
        "semantic_mismatches": semantic,
    }
    return body | {"report_digest": _digest(body)}
