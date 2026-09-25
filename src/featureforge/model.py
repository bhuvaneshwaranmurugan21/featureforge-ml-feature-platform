"""Transport-neutral feature platform domain objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from featureforge.canonical import digest

ValueType = Literal["integer", "float"]


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    version: int
    value_type: ValueType
    ttl_seconds: int
    computation: str
    window_seconds: int | None
    description: str
    empty_default: int | float | None = None

    def __post_init__(self) -> None:
        if self.version < 1 or self.ttl_seconds <= 0:
            raise ValueError("feature version and TTL must be positive")
        if self.window_seconds is not None and self.window_seconds <= 0:
            raise ValueError("window must be positive")
        if self.empty_default is not None:
            if self.value_type == "integer" and (
                not isinstance(self.empty_default, int) or isinstance(self.empty_default, bool)
            ):
                raise ValueError("integer feature default must be an integer or null")
            if self.value_type == "float" and not isinstance(self.empty_default, (int, float)):
                raise ValueError("float feature default must be numeric or null")

    @property
    def definition_id(self) -> str:
        return f"{self.name}:v{self.version}"

    @property
    def definition_digest(self) -> str:
        return digest(
            {
                "computation": self.computation,
                "description": self.description,
                "empty_default": self.empty_default,
                "name": self.name,
                "ttl_seconds": self.ttl_seconds,
                "value_type": self.value_type,
                "version": self.version,
                "window_seconds": self.window_seconds,
            }
        )


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    customer_id: str
    event_time: int
    knowledge_time: int
    amount_cents: int | None
    status: Literal["succeeded", "failed"] | None
    merchant_risk: float | None
    revision_id: str | None = None
    operation: Literal["upsert", "retract"] = "upsert"

    def __post_init__(self) -> None:
        if not self.event_id or not self.customer_id:
            raise ValueError("event and customer identities must be non-empty")
        if self.event_time < 0 or self.knowledge_time < 0:
            raise ValueError("timestamps must be non-negative epoch seconds")
        if self.knowledge_time < self.event_time:
            raise ValueError("knowledge time cannot precede event time")
        if self.revision_id is None:
            object.__setattr__(self, "revision_id", f"{self.event_id}@{self.knowledge_time}")
        if not self.revision_id:
            raise ValueError("revision identity must be non-empty")
        if self.operation not in {"upsert", "retract"}:
            raise ValueError("unsupported payment-event operation")
        if self.operation == "retract":
            if any(
                value is not None
                for value in (self.amount_cents, self.status, self.merchant_risk)
            ):
                raise ValueError("retraction must not contain a payment payload")
            return
        if self.amount_cents is None or self.amount_cents < 0:
            raise ValueError("upsert amount must be non-negative")
        if self.status not in {"succeeded", "failed"}:
            raise ValueError("upsert status is invalid")
        if self.merchant_risk is None or not 0 <= self.merchant_risk <= 1:
            raise ValueError("upsert merchant risk must be between zero and one")

    def as_dict(self) -> dict[str, Any]:
        return {
            "amount_cents": self.amount_cents,
            "customer_id": self.customer_id,
            "event_id": self.event_id,
            "event_time": self.event_time,
            "knowledge_time": self.knowledge_time,
            "merchant_risk": self.merchant_risk,
            "operation": self.operation,
            "revision_id": self.revision_id,
            "status": self.status,
        }


@dataclass(frozen=True)
class Label:
    label_id: str
    customer_id: str
    label_time: int
    value: int
    prediction_knowledge_time: int | None = None

    def __post_init__(self) -> None:
        if not self.label_id or not self.customer_id:
            raise ValueError("label and customer identities must be non-empty")
        if self.label_time < 0:
            raise ValueError("label time must be non-negative")
        if self.prediction_knowledge_time is None:
            object.__setattr__(self, "prediction_knowledge_time", self.label_time)
        if self.prediction_knowledge_time is None or self.prediction_knowledge_time < 0:
            raise ValueError("prediction knowledge time must be non-negative")
        if self.prediction_knowledge_time > self.label_time:
            raise ValueError("prediction knowledge time cannot follow label time")


@dataclass(frozen=True)
class FeatureValue:
    customer_id: str
    feature_name: str
    value: int | float | None
    event_time: int
    knowledge_time: int
    definition_digest: str
    generation_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "definition_digest": self.definition_digest,
            "event_time": self.event_time,
            "feature_name": self.feature_name,
            "generation_id": self.generation_id,
            "knowledge_time": self.knowledge_time,
            "value": self.value,
        }
