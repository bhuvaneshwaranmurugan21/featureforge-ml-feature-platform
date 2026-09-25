from __future__ import annotations

from dataclasses import replace

import pytest

from featureforge.computation import latest_known_events
from featureforge.model import PaymentEvent
from featureforge.store import FeatureStore, ReplayConflict


def test_store_persists_revisions_and_retractions_without_losing_history() -> None:
    store = FeatureStore()
    original = PaymentEvent("event", "customer", 10, 11, 100, "succeeded", 0.2, "r1")
    correction = replace(original, revision_id="r2", knowledge_time=12, amount_cents=150)
    retraction = PaymentEvent(
        "event", "customer", 10, 13, None, None, None, "r3", "retract"
    )
    assert store.append_event(original) == "appended"
    assert store.append_event(correction) == "appended"
    assert store.append_event(retraction) == "appended"
    assert store.append_event(original) == "replayed"
    history = store.events()
    assert [event.revision_id for event in history] == ["r1", "r2", "r3"]
    assert latest_known_events(
        history, "customer", event_cutoff=10, knowledge_cutoff=11
    ) == (original,)
    assert latest_known_events(
        history, "customer", event_cutoff=10, knowledge_cutoff=12
    ) == (correction,)
    assert latest_known_events(history, "customer", event_cutoff=10, knowledge_cutoff=13) == ()
    store.close()


def test_store_rejects_revision_content_same_clock_and_customer_conflicts() -> None:
    store = FeatureStore()
    original = PaymentEvent("event", "customer", 10, 11, 100, "succeeded", 0.2, "r1")
    store.append_event(original)
    with pytest.raises(ReplayConflict, match="different content"):
        store.append_event(replace(original, amount_cents=101))
    with pytest.raises(ReplayConflict, match="same knowledge time"):
        store.append_event(replace(original, revision_id="r2", amount_cents=101))
    with pytest.raises(ReplayConflict, match="customer identity"):
        store.append_event(
            replace(
                original,
                revision_id="r3",
                customer_id="another",
                knowledge_time=12,
            )
        )
    store.close()
