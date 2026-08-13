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

    def __post_init__(self) -> None:
        if self.version < 1 or self.ttl_seconds <= 0:
            raise ValueError("feature version and TTL must be positive")
        if self.window_seconds is not None and self.window_seconds <= 0:
            raise ValueError("window must be positive")

    @property
    def definition_id(self) -> str:
        return f"{self.name}:v{self.version}"

    @property
    def definition_digest(self) -> str:
        return digest(
            {
                "computation": self.computation,
                "description": self.description,
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
    amount_cents: int
    status: Literal["succeeded", "failed"]
    merchant_risk: float

    def __post_init__(self) -> None:
        if self.event_time < 0 or self.knowledge_time < 0:
            raise ValueError("timestamps must be non-negative epoch seconds")
        if self.amount_cents < 0:
            raise ValueError("amount cannot be negative")
        if not 0 <= self.merchant_risk <= 1:
            raise ValueError("merchant risk must be between zero and one")

    def as_dict(self) -> dict[str, Any]:
        return {
            "amount_cents": self.amount_cents,
            "customer_id": self.customer_id,
            "event_id": self.event_id,
            "event_time": self.event_time,
            "knowledge_time": self.knowledge_time,
            "merchant_risk": self.merchant_risk,
            "status": self.status,
        }


@dataclass(frozen=True)
class Label:
    label_id: str
    customer_id: str
    label_time: int
    value: int


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
