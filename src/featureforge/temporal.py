"""Fail-closed bitemporal revision normalization and selection."""

from __future__ import annotations

from collections.abc import Iterable

from featureforge.model import PaymentEvent


class TemporalContractError(ValueError):
    """A source history cannot be interpreted without inventing temporal order."""


def normalize_events(events: Iterable[PaymentEvent]) -> tuple[PaymentEvent, ...]:
    """Canonicalize exact replay and reject ambiguous logical-event histories."""

    revisions: dict[str, PaymentEvent] = {}
    customers: dict[str, str] = {}
    event_clocks: dict[tuple[str, int], str] = {}
    for event in events:
        revision_id = event.revision_id
        if revision_id is None:  # PaymentEvent normalizes this in __post_init__.
            raise TemporalContractError("revision identity was not normalized")
        prior_revision = revisions.get(revision_id)
        if prior_revision is not None:
            if prior_revision != event:
                raise TemporalContractError(
                    f"revision_id {revision_id!r} was reused with different content"
                )
            continue

        prior_customer = customers.setdefault(event.event_id, event.customer_id)
        if prior_customer != event.customer_id:
            raise TemporalContractError(
                f"logical event {event.event_id!r} changed customer identity"
            )

        clock = (event.event_id, event.knowledge_time)
        competing_revision = event_clocks.get(clock)
        if competing_revision is not None and competing_revision != revision_id:
            raise TemporalContractError(
                f"logical event {event.event_id!r} has conflicting revisions at "
                f"knowledge_time={event.knowledge_time}"
            )
        event_clocks[clock] = revision_id
        revisions[revision_id] = event

    return tuple(
        sorted(
            revisions.values(),
            key=lambda event: (event.event_id, event.knowledge_time, event.revision_id or ""),
        )
    )


def select_events(
    events: Iterable[PaymentEvent],
    customer_id: str,
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[PaymentEvent, ...]:
    """Select the latest knowable non-retracted state, then apply business time."""

    if event_cutoff < 0 or knowledge_cutoff < 0:
        raise ValueError("cutoffs must be non-negative epoch seconds")
    latest: dict[str, PaymentEvent] = {}
    for event in normalize_events(events):
        if event.knowledge_time <= knowledge_cutoff:
            latest[event.event_id] = event
    return tuple(
        sorted(
            (
                event
                for event in latest.values()
                if event.operation == "upsert"
                and event.customer_id == customer_id
                and event.event_time <= event_cutoff
            ),
            key=lambda event: (event.event_time, event.event_id, event.revision_id or ""),
        )
    )
