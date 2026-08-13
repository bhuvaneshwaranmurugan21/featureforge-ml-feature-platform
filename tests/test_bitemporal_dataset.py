from dataclasses import replace

from featureforge.computation import latest_known_events, materialize
from featureforge.dataset import build_training_dataset
from featureforge.definitions import payment_features
from featureforge.model import Label
from featureforge.simulator import events_fixture


def test_point_in_time_join_blocks_future_and_late_knowledge() -> None:
    dataset = build_training_dataset(
        "g",
        (Label("l-1", "c-1", 3_000, 1),),
        payment_features(),
        events_fixture(),
        dataset_as_of=5_000,
    )
    row = dataset.rows[0]
    assert row["transaction_count_24h"] == 2
    assert row["successful_spend_30d_cents"] == 1_000
    assert row["knowledge_cutoff"] == 3_000


def test_dataset_manifest_is_reproducible_and_definition_bound() -> None:
    args = (
        "g",
        (Label("l-1", "c-1", 3_000, 1),),
        payment_features(),
        events_fixture(),
    )
    first = build_training_dataset(*args, dataset_as_of=5_000)
    second = build_training_dataset(*args, dataset_as_of=5_000)
    assert first.manifest == second.manifest
    changed = tuple(
        replace(item, version=2) if item.name == "transaction_count_24h" else item
        for item in payment_features()
    )
    third = build_training_dataset("g", args[1], changed, args[3], dataset_as_of=5_000)
    assert first.manifest["manifest_digest"] != third.manifest["manifest_digest"]


def test_latest_revision_depends_on_knowledge_cutoff() -> None:
    events = events_fixture()
    before = latest_known_events(events, "c-1", event_cutoff=5_000, knowledge_cutoff=3_000)
    after = latest_known_events(events, "c-1", event_cutoff=5_000, knowledge_cutoff=5_000)
    assert next(event.amount_cents for event in before if event.event_id == "p-1") == 1_000
    assert next(event.amount_cents for event in after if event.event_id == "p-1") == 9_000


def test_missing_entity_has_explicit_defaults() -> None:
    values = materialize(
        "g",
        payment_features(),
        events_fixture(),
        ("missing",),
        event_cutoff=5_000,
        knowledge_cutoff=5_000,
    )
    assert any(value.value == 0 for value in values)
    assert any(value.value is None for value in values)
