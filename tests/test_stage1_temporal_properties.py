from __future__ import annotations

from dataclasses import replace
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from featureforge.computation import latest_known_events
from featureforge.model import PaymentEvent
from tests.temporal_oracle import select as oracle_select


def _records(events: tuple[PaymentEvent, ...]) -> list[dict[str, Any]]:
    return [event.as_dict() for event in events]


@settings(max_examples=200, derandomize=True, deadline=None)
@given(
    event_time=st.integers(min_value=0, max_value=10_000),
    first_lag=st.integers(min_value=0, max_value=100),
    correction_lag=st.integers(min_value=1, max_value=100),
    original_amount=st.integers(min_value=0, max_value=1_000_000),
    corrected_amount=st.integers(min_value=0, max_value=1_000_000),
)
def test_future_correction_cannot_rewrite_prior_cutoff(
    event_time: int,
    first_lag: int,
    correction_lag: int,
    original_amount: int,
    corrected_amount: int,
) -> None:
    first_knowledge = event_time + first_lag
    correction_knowledge = first_knowledge + correction_lag
    original = PaymentEvent(
        "event", "customer", event_time, first_knowledge, original_amount, "succeeded", 0.2, "r1"
    )
    correction = replace(
        original,
        revision_id="r2",
        knowledge_time=correction_knowledge,
        amount_cents=corrected_amount,
    )
    before = latest_known_events(
        (original, correction),
        "customer",
        event_cutoff=event_time,
        knowledge_cutoff=first_knowledge,
    )
    after = latest_known_events(
        (correction, original),
        "customer",
        event_cutoff=event_time,
        knowledge_cutoff=correction_knowledge,
    )
    assert before == (original,)
    assert after == (correction,)
    oracle_before = oracle_select(
        _records((original, correction)), "customer", event_time, first_knowledge
    )
    assert [row["revision_id"] for row in oracle_before] == ["r1"]


@settings(max_examples=150, derandomize=True, deadline=None)
@given(
    event_time=st.integers(min_value=0, max_value=10_000),
    first_lag=st.integers(min_value=0, max_value=100),
    retraction_lag=st.integers(min_value=1, max_value=100),
)
def test_retraction_preserves_earlier_history(
    event_time: int, first_lag: int, retraction_lag: int
) -> None:
    first_knowledge = event_time + first_lag
    retraction_knowledge = first_knowledge + retraction_lag
    original = PaymentEvent(
        "event", "customer", event_time, first_knowledge, 100, "succeeded", 0.2, "r1"
    )
    retraction = PaymentEvent(
        "event",
        "customer",
        event_time,
        retraction_knowledge,
        None,
        None,
        None,
        "r2",
        "retract",
    )
    before = latest_known_events(
        (retraction, original),
        "customer",
        event_cutoff=event_time,
        knowledge_cutoff=first_knowledge,
    )
    after = latest_known_events(
        (original, retraction),
        "customer",
        event_cutoff=event_time,
        knowledge_cutoff=retraction_knowledge,
    )
    assert before == (original,)
    assert after == ()


def test_bounded_small_history_matches_independent_oracle() -> None:
    cases = 0
    for event_time in (10, 11, 12):
        for knowledge_time in (12, 13, 14):
            if knowledge_time < event_time:
                continue
            for cutoff in (11, 12, 13, 14):
                event = PaymentEvent(
                    f"e-{event_time}",
                    "customer",
                    event_time,
                    knowledge_time,
                    event_time * 10,
                    "succeeded" if event_time % 2 == 0 else "failed",
                    0.5,
                    f"r-{event_time}-{knowledge_time}",
                )
                production = latest_known_events(
                    (event,), "customer", event_cutoff=12, knowledge_cutoff=cutoff
                )
                reference = oracle_select(_records((event,)), "customer", 12, cutoff)
                assert [item.revision_id for item in production] == [
                    item["revision_id"] for item in reference
                ]
                cases += 1
    assert cases == 36
