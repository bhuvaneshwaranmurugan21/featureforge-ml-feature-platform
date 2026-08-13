"""One computation library shared by training and serving materialization."""

from __future__ import annotations

from collections.abc import Iterable

from featureforge.model import FeatureDefinition, FeatureValue, PaymentEvent


class FeatureTypeError(TypeError):
    pass


def latest_known_events(
    events: Iterable[PaymentEvent],
    customer_id: str,
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[PaymentEvent, ...]:
    """Resolve the latest revision known by the cutoff, then apply business-time cutoff."""

    revisions: dict[str, PaymentEvent] = {}
    for event in events:
        if event.customer_id != customer_id or event.knowledge_time > knowledge_cutoff:
            continue
        previous = revisions.get(event.event_id)
        if previous is None or event.knowledge_time > previous.knowledge_time:
            revisions[event.event_id] = event
    return tuple(
        sorted(
            (event for event in revisions.values() if event.event_time <= event_cutoff),
            key=lambda event: (event.event_time, event.event_id),
        )
    )


def _window(
    events: tuple[PaymentEvent, ...], cutoff: int, seconds: int | None
) -> tuple[PaymentEvent, ...]:
    if seconds is None:
        return events
    return tuple(event for event in events if event.event_time > cutoff - seconds)


def compute(
    definition: FeatureDefinition, events: tuple[PaymentEvent, ...], cutoff: int
) -> int | float | None:
    candidates = _window(events, cutoff, definition.window_seconds)
    if definition.computation == "transaction_count":
        value: int | float | None = len(candidates)
    elif definition.computation == "successful_spend_cents":
        value = sum(event.amount_cents for event in candidates if event.status == "succeeded")
    elif definition.computation == "failed_payment_ratio":
        value = (
            sum(event.status == "failed" for event in candidates) / len(candidates)
            if candidates
            else 0.0
        )
    elif definition.computation == "hours_since_success":
        successes = [event.event_time for event in candidates if event.status == "succeeded"]
        value = (cutoff - max(successes)) / 3600 if successes else None
    elif definition.computation == "max_merchant_risk":
        value = max((event.merchant_risk for event in candidates), default=None)
    else:
        raise ValueError(f"unknown computation: {definition.computation}")
    validate_value(definition, value)
    return value


def validate_value(definition: FeatureDefinition, value: int | float | None) -> None:
    if value is None:
        return
    if definition.value_type == "integer" and (
        not isinstance(value, int) or isinstance(value, bool)
    ):
        raise FeatureTypeError(f"{definition.name} requires integer, got {type(value).__name__}")
    if definition.value_type == "float" and not isinstance(value, (int, float)):
        raise FeatureTypeError(f"{definition.name} requires float, got {type(value).__name__}")


def materialize(
    generation_id: str,
    definitions: tuple[FeatureDefinition, ...],
    events: tuple[PaymentEvent, ...],
    customer_ids: tuple[str, ...],
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[FeatureValue, ...]:
    values: list[FeatureValue] = []
    for customer_id in sorted(customer_ids):
        known = latest_known_events(
            events,
            customer_id,
            event_cutoff=event_cutoff,
            knowledge_cutoff=knowledge_cutoff,
        )
        for definition in definitions:
            values.append(
                FeatureValue(
                    customer_id=customer_id,
                    feature_name=definition.name,
                    value=compute(definition, known, event_cutoff),
                    event_time=event_cutoff,
                    knowledge_time=knowledge_cutoff,
                    definition_digest=definition.definition_digest,
                    generation_id=generation_id,
                )
            )
    return tuple(values)
