from dataclasses import replace

import pytest

from featureforge.canonical import digest
from featureforge.computation import materialize
from featureforge.definitions import payment_features
from featureforge.model import FeatureDefinition
from featureforge.parity import compare_parity
from featureforge.simulator import events_fixture
from featureforge.store import (
    CompareAndSwapConflict,
    FeatureStore,
    GenerationConflict,
    ReplayConflict,
)


def _prepared_store() -> tuple[FeatureStore, tuple[FeatureDefinition, ...]]:
    store = FeatureStore()
    definitions = payment_features()
    for event in events_fixture():
        store.append_event(event)
    store.create_generation(
        "g-1",
        digest(sorted(item.definition_digest for item in definitions)),
        source_frontier=5_000,
        dataset_as_of=5_000,
    )
    values = materialize(
        "g-1", definitions, store.events(), ("c-1",), event_cutoff=5_000, knowledge_cutoff=5_000
    )
    store.put_offline(values)
    return store, definitions


def test_event_and_materialization_replay() -> None:
    store, _ = _prepared_store()
    event = events_fixture()[0]
    assert store.append_event(event) == "replayed"
    with pytest.raises(ReplayConflict):
        store.append_event(replace(event, amount_cents=999))
    assert store.put_offline(store.offline_values("g-1")) == "replayed"
    store.close()


def test_parity_gates_atomic_publication_and_ttl() -> None:
    store, definitions = _prepared_store()
    store.stage_online("g-1")
    report = compare_parity(store.offline_values("g-1"), store.online_values("g-1"))
    assert report.matched
    with pytest.raises(GenerationConflict):
        store.mark_ready("g-1", parity_matched=False)
    store.mark_ready("g-1", parity_matched=True)
    assert store.activate("g-1", 0) == 1
    assert (
        store.serve(
            "c-1",
            "transaction_count_24h",
            request_time=5_001,
            ttl_seconds=definitions[0].ttl_seconds,
        )
        == 3
    )
    assert store.serve(
        "c-1",
        "transaction_count_24h",
        request_time=5_000 + definitions[0].ttl_seconds,
        ttl_seconds=definitions[0].ttl_seconds,
    ) is None
    store.close()


def test_stale_publication_is_rejected() -> None:
    store, definitions = _prepared_store()
    store.stage_online("g-1")
    store.mark_ready("g-1", parity_matched=True)
    store.activate("g-1", 0)
    store.create_generation(
        "g-2",
        digest(sorted(item.definition_digest for item in definitions)),
        source_frontier=5_000,
        dataset_as_of=5_000,
    )
    values = materialize(
        "g-2", definitions, store.events(), ("c-1",), event_cutoff=5_000, knowledge_cutoff=5_000
    )
    store.put_offline(values)
    store.stage_online("g-2")
    store.mark_ready("g-2", parity_matched=True)
    with pytest.raises(CompareAndSwapConflict):
        store.activate("g-2", 0)
    store.close()


def test_parity_detects_exact_value_difference() -> None:
    store, _ = _prepared_store()
    store.stage_online("g-1")
    online = store.online_values("g-1")
    changed = tuple(
        replace(value, value=-1) if value.feature_name == "transaction_count_24h" else value
        for value in online
    )
    report = compare_parity(store.offline_values("g-1"), changed)
    assert not report.matched
    assert report.mismatches
    store.close()
