"""Stage 5 activation, freshness, telemetry, and measurement authorities."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Literal, Protocol

from featureforge.canonical import digest

ACTIVATION_DECISION_CONTRACT = "stage5-activation-decision-v1"
TELEMETRY_CONTRACT = "stage5-telemetry-event-v1"
BENCHMARK_TRIAL_CONTRACT = "stage5-benchmark-trial-v1"


class AssuranceError(ValueError):
    """Stage 5 evidence or policy inputs violate the contract."""


class ActivationRejected(RuntimeError):
    """The composite Stage 5 gate rejected candidate activation."""


@dataclass(frozen=True)
class ActivationGateInputs:
    feature_set: str
    generation_id: str
    operation_id: str
    validation_receipt_digest: str
    expected_generation: str | None
    expected_version: int
    evaluated_at: int
    evidence_digests: tuple[str, ...]
    source_complete: bool
    manifests_valid: bool
    offline_valid: bool
    online_reconciled: bool
    parity_passed: bool
    freshness_passed: bool
    candidate_consistent: bool
    unique_candidate: bool
    pointer_current: bool

    def __post_init__(self) -> None:
        if (
            not self.feature_set
            or not self.generation_id
            or not self.operation_id
            or not self.validation_receipt_digest
            or self.expected_version < 0
            or self.evaluated_at < 0
            or not self.evidence_digests
        ):
            raise AssuranceError("activation gate identities are incomplete")
        if any(len(value) != 64 for value in self.evidence_digests):
            raise AssuranceError("activation evidence digest is malformed")


@dataclass(frozen=True)
class ActivationDecision:
    feature_set: str
    generation_id: str
    operation_id: str
    validation_receipt_digest: str
    expected_generation: str | None
    expected_version: int
    evaluated_at: int
    evidence_digests: tuple[str, ...]
    input_digest: str
    status: Literal["ELIGIBLE", "REJECTED"]
    reason_codes: tuple[str, ...]
    decision_digest: str

    def body(self) -> dict[str, Any]:
        return {
            "contract": ACTIVATION_DECISION_CONTRACT,
            "evaluated_at": self.evaluated_at,
            "evidence_digests": list(self.evidence_digests),
            "expected_generation": self.expected_generation,
            "expected_version": self.expected_version,
            "feature_set": self.feature_set,
            "generation_id": self.generation_id,
            "input_digest": self.input_digest,
            "operation_id": self.operation_id,
            "reason_codes": list(self.reason_codes),
            "status": self.status,
            "validation_receipt_digest": self.validation_receipt_digest,
        }

    def as_dict(self) -> dict[str, Any]:
        return self.body() | {"decision_digest": self.decision_digest}

    def verified(self) -> bool:
        return self.decision_digest == digest(self.body())


_GATE_REASONS = (
    ("source_complete", "SOURCE_INCOMPLETE"),
    ("manifests_valid", "MANIFEST_INVALID"),
    ("offline_valid", "OFFLINE_INVALID"),
    ("online_reconciled", "ONLINE_RECONCILIATION_FAILED"),
    ("parity_passed", "PARITY_FAILED"),
    ("freshness_passed", "FRESHNESS_FAILED"),
    ("candidate_consistent", "CANDIDATE_INCONSISTENT"),
    ("unique_candidate", "DUPLICATE_OR_CONFLICTING_CANDIDATE"),
    ("pointer_current", "STALE_POINTER"),
)


def evaluate_activation(inputs: ActivationGateInputs) -> ActivationDecision:
    """Evaluate all activation prerequisites as one ordered conjunction."""

    reasons = tuple(reason for field, reason in _GATE_REASONS if not getattr(inputs, field))
    input_body = asdict(inputs)
    input_body["evidence_digests"] = list(inputs.evidence_digests)
    input_digest = digest(input_body)
    status: Literal["ELIGIBLE", "REJECTED"] = "REJECTED" if reasons else "ELIGIBLE"
    partial = {
        "contract": ACTIVATION_DECISION_CONTRACT,
        "evaluated_at": inputs.evaluated_at,
        "evidence_digests": list(inputs.evidence_digests),
        "expected_generation": inputs.expected_generation,
        "expected_version": inputs.expected_version,
        "feature_set": inputs.feature_set,
        "generation_id": inputs.generation_id,
        "input_digest": input_digest,
        "operation_id": inputs.operation_id,
        "reason_codes": list(reasons),
        "status": status,
        "validation_receipt_digest": inputs.validation_receipt_digest,
    }
    return ActivationDecision(
        feature_set=inputs.feature_set,
        generation_id=inputs.generation_id,
        operation_id=inputs.operation_id,
        validation_receipt_digest=inputs.validation_receipt_digest,
        expected_generation=inputs.expected_generation,
        expected_version=inputs.expected_version,
        evaluated_at=inputs.evaluated_at,
        evidence_digests=inputs.evidence_digests,
        input_digest=input_digest,
        status=status,
        reason_codes=reasons,
        decision_digest=digest(partial),
    )


class DecisionStore(Protocol):
    def activate_with_decision(
        self,
        decision: Mapping[str, Any],
        *,
        actor: str,
        reason: str,
        lose_acknowledgement: bool = False,
    ) -> dict[str, Any]: ...


def activate_eligible(
    store: DecisionStore,
    decision: ActivationDecision,
    *,
    actor: str,
    reason: str,
    lose_acknowledgement: bool = False,
) -> dict[str, Any]:
    if not decision.verified():
        raise AssuranceError("activation decision digest is invalid")
    if decision.status != "ELIGIBLE":
        raise ActivationRejected(
            "candidate activation rejected: " + ",".join(decision.reason_codes)
        )
    return store.activate_with_decision(
        decision.as_dict(),
        actor=actor,
        reason=reason,
        lose_acknowledgement=lose_acknowledgement,
    )


@dataclass(frozen=True)
class LagSnapshot:
    source_arrival_lag: int
    build_duration: int
    materialization_duration: int
    build_materialization_lag: int
    activation_lag: int
    online_freshness_age: int
    ttl_age: int
    end_to_end_availability_lag: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def calculate_lags(
    *,
    event_time: int,
    knowledge_time: int,
    source_frontier_available_at: int,
    build_started_monotonic: int,
    build_completed_monotonic: int,
    materialization_started_monotonic: int,
    materialization_completed_monotonic: int,
    candidate_validated_at: int,
    activated_at: int,
    request_time: int,
    record_frontier: int,
    expires_at: int,
) -> LagSnapshot:
    values = (
        event_time,
        knowledge_time,
        source_frontier_available_at,
        build_started_monotonic,
        build_completed_monotonic,
        materialization_started_monotonic,
        materialization_completed_monotonic,
        candidate_validated_at,
        activated_at,
        request_time,
        record_frontier,
        expires_at,
    )
    if any(value < 0 for value in values):
        raise AssuranceError("clock values must be non-negative")
    differences = (
        knowledge_time - event_time,
        build_completed_monotonic - build_started_monotonic,
        materialization_completed_monotonic - materialization_started_monotonic,
        activated_at - candidate_validated_at,
        request_time - record_frontier,
        request_time - event_time,
    )
    if any(value < 0 for value in differences):
        raise AssuranceError("clock ordering regressed")
    return LagSnapshot(
        source_arrival_lag=differences[0],
        build_duration=differences[1],
        materialization_duration=differences[2],
        build_materialization_lag=(
            materialization_completed_monotonic - source_frontier_available_at
        ),
        activation_lag=differences[3],
        online_freshness_age=differences[4],
        ttl_age=request_time - expires_at,
        end_to_end_availability_lag=differences[5],
    )


ALLOWED_EVENTS = frozenset(
    {
        "source.completeness.checked",
        "spark.build.completed",
        "spark.scope.planned",
        "candidate.materialization.completed",
        "candidate.retry.recorded",
        "candidate.reconciliation.completed",
        "candidate.parity.completed",
        "candidate.freshness.completed",
        "candidate.activation.rejected",
        "candidate.activation.committed",
        "serving.outcome",
        "generation.rollback.completed",
        "generation.retirement.eligible",
    }
)
ALLOWED_LABELS = frozenset({"phase", "profile", "reason_code", "size_class", "status"})
FORBIDDEN_LABELS = frozenset(
    {"customer_id", "entity_id", "request_id", "operation_id", "generation_id", "exception"}
)
REQUIRED_TRACE = frozenset(
    {"definition_set_digest", "generation_id", "operation_id", "source_frontier_digest"}
)


@dataclass(frozen=True)
class TelemetryEvent:
    event: str
    occurred_at: int
    labels: Mapping[str, str]
    trace: Mapping[str, str]
    measurements: Mapping[str, int | float]

    def as_dict(self) -> dict[str, Any]:
        body = {
            "contract": TELEMETRY_CONTRACT,
            "event": self.event,
            "labels": dict(sorted(self.labels.items())),
            "measurements": dict(sorted(self.measurements.items())),
            "occurred_at": self.occurred_at,
            "trace": dict(sorted(self.trace.items())),
        }
        return body | {"event_digest": digest(body)}


def validate_telemetry(event: TelemetryEvent) -> None:
    if event.event not in ALLOWED_EVENTS or event.occurred_at < 0:
        raise AssuranceError("telemetry event identity is invalid")
    label_keys = set(event.labels)
    if label_keys - ALLOWED_LABELS or label_keys & FORBIDDEN_LABELS:
        raise AssuranceError("telemetry contains an unbounded or forbidden label")
    if not set(event.trace) >= REQUIRED_TRACE:
        raise AssuranceError("telemetry correlation set is incomplete")
    strings = (*event.labels.values(), *event.trace.values())
    if any(not value or len(value) > 128 for value in strings):
        raise AssuranceError("telemetry string field is empty or oversized")
    if any(not math.isfinite(float(value)) for value in event.measurements.values()):
        raise AssuranceError("telemetry measurement is not finite")


@dataclass(frozen=True)
class BenchmarkTrial:
    case_id: str
    operation: str
    profile: str
    size_class: str
    trial: int
    warm: bool
    duration_ns: int
    input_digest: str
    output_digest: str
    correctness_passed: bool

    def as_dict(self) -> dict[str, Any]:
        body = {"contract": BENCHMARK_TRIAL_CONTRACT, **asdict(self)}
        return body | {"trial_digest": digest(body)}


def summarize_trials(trials: Sequence[BenchmarkTrial]) -> dict[str, Any]:
    if not trials:
        raise AssuranceError("benchmark summary requires trials")
    if any(
        trial.duration_ns <= 0 or not trial.correctness_passed or trial.trial < 0
        for trial in trials
    ):
        raise AssuranceError("benchmark includes invalid or incorrect trial")
    grouped: dict[tuple[str, str], list[BenchmarkTrial]] = {}
    for trial in trials:
        grouped.setdefault((trial.case_id, trial.operation), []).append(trial)
    summaries: list[dict[str, Any]] = []
    for (case_id, operation), rows in sorted(grouped.items()):
        warm = [row.duration_ns for row in rows if row.warm]
        cold = [row.duration_ns for row in rows if not row.warm]
        if len(warm) < 5 or len(cold) < 1:
            raise AssuranceError("each benchmark operation needs one cold and five warm trials")
        ordered = sorted(warm)
        lower = statistics.median(ordered[: len(ordered) // 2])
        upper = statistics.median(ordered[(len(ordered) + 1) // 2 :])
        median = statistics.median(ordered)
        mean = statistics.fmean(ordered)
        deviation = statistics.pstdev(ordered)
        summaries.append(
            {
                "case_id": case_id,
                "cold_ns": cold,
                "coefficient_of_variation": 0.0 if mean == 0 else deviation / mean,
                "input_digests": sorted({row.input_digest for row in rows}),
                "maximum_ns": max(ordered),
                "median_ns": int(median),
                "minimum_ns": min(ordered),
                "operation": operation,
                "output_digests": sorted({row.output_digest for row in rows}),
                "warm_interquartile_range_ns": int(upper - lower),
                "warm_trial_count": len(warm),
            }
        )
    body = {"contract": "stage5-benchmark-summary-v1", "summaries": summaries}
    return body | {"summary_digest": digest(body)}
