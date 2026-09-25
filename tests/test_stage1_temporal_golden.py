from __future__ import annotations

import itertools
import json
from dataclasses import replace
from pathlib import Path

import pytest

from featureforge.computation import compute, latest_known_events
from featureforge.dataset import build_training_dataset
from featureforge.model import FeatureDefinition, Label, PaymentEvent
from featureforge.temporal import TemporalContractError, normalize_events
from tests.temporal_oracle import OracleConflict
from tests.temporal_oracle import select as oracle_select
from tests.temporal_oracle import values as oracle_values

FIXTURE_PATH = Path(__file__).parent / "fixtures/stage1-temporal-cases.json"


def _fixture() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _events(data: dict[str, object]) -> tuple[PaymentEvent, ...]:
    records = data["events"]
    assert isinstance(records, list)
    return tuple(PaymentEvent(**record) for record in records)


def _definitions(data: dict[str, object]) -> tuple[FeatureDefinition, ...]:
    records = data["definitions"]
    assert isinstance(records, list)
    return tuple(FeatureDefinition(**record) for record in records)


def _production(
    data: dict[str, object], knowledge_cutoff: int
) -> tuple[list[str | None], dict[str, int | float | None]]:
    events = latest_known_events(
        _events(data),
        str(data["customer_id"]),
        event_cutoff=int(data["event_cutoff"]),
        knowledge_cutoff=knowledge_cutoff,
    )
    definitions = _definitions(data)
    values = {
        definition.name: compute(definition, events, int(data["event_cutoff"]))
        for definition in definitions
    }
    return [event.revision_id for event in events], values


def test_hand_calculated_golden_cases_match_production_and_independent_oracle() -> None:
    data = _fixture()
    raw_events = data["events"]
    raw_definitions = data["definitions"]
    assert isinstance(raw_events, list) and isinstance(raw_definitions, list)
    cases = data["cases"]
    assert isinstance(cases, list)
    for case in cases:
        cutoff = int(case["knowledge_cutoff"])
        expected = case["selected_revision_ids"], case["values"]
        assert _production(data, cutoff) == expected
        selected = oracle_select(
            raw_events,
            str(data["customer_id"]),
            int(data["event_cutoff"]),
            cutoff,
        )
        actual = [row["revision_id"] for row in selected], oracle_values(
            raw_definitions, selected, int(data["event_cutoff"])
        )
        assert actual == expected


def test_deliberately_leaky_event_time_only_join_is_caught() -> None:
    data = _fixture()
    latest = {event.event_id: event for event in normalize_events(_events(data))}
    selected = tuple(
        event
        for event in latest.values()
        if event.operation == "upsert"
        and int(data["event_cutoff"]) - int(data["window_seconds"])
        < event.event_time
        <= int(data["event_cutoff"])
    )
    spend = next(item for item in _definitions(data) if item.name == "spend_100s")
    wrong = compute(spend, selected, int(data["event_cutoff"]))
    leaky = data["leaky_event_time_only"]
    assert isinstance(leaky, dict)
    assert wrong == leaky["wrong_spend_100s"]
    assert wrong != next(case for case in data["cases"] if case["name"] == "before_correction")[
        "values"
    ]["spend_100s"]


def test_exhaustive_input_permutations_and_exact_replay_are_invariant() -> None:
    data = _fixture()
    events = _events(data)
    baseline = _production(data, 205)
    for permutation in itertools.permutations(events):
        known = latest_known_events(
            permutation, "c-1", event_cutoff=200, knowledge_cutoff=205
        )
        values = {
            definition.name: compute(definition, known, 200)
            for definition in _definitions(data)
        }
        assert ([event.revision_id for event in known], values) == baseline
    replayed = events + (events[1], events[1])
    known = latest_known_events(replayed, "c-1", event_cutoff=200, knowledge_cutoff=205)
    assert [event.revision_id for event in known] == baseline[0]


def test_invalid_and_ambiguous_histories_fail_closed() -> None:
    data = _fixture()
    events = _events(data)
    with pytest.raises(TemporalContractError, match="reused with different content"):
        normalize_events(events + (replace(events[1], amount_cents=101),))
    with pytest.raises(TemporalContractError, match="conflicting revisions"):
        normalize_events(
            events + (replace(events[2], revision_id="p-1-r3", amount_cents=151),)
        )
    with pytest.raises(TemporalContractError, match="changed customer identity"):
        normalize_events(
            events
            + (
                replace(
                    events[2],
                    revision_id="p-1-r4",
                    customer_id="c-2",
                    knowledge_time=191,
                ),
            )
        )


def test_model_rejects_invalid_clocks_and_retraction_payload() -> None:
    with pytest.raises(ValueError, match="cannot precede"):
        PaymentEvent("bad", "c-1", 10, 9, 1, "succeeded", 0.1, "bad-r1")
    with pytest.raises(ValueError, match="must not contain"):
        PaymentEvent("bad", "c-1", 10, 11, 1, None, None, "bad-r2", "retract")
    invalid_clock = {
        "event_id": "bad",
        "revision_id": "bad-r1",
        "customer_id": "c-1",
        "event_time": 10,
        "knowledge_time": 9,
        "operation": "upsert",
        "amount_cents": 1,
        "status": "succeeded",
        "merchant_risk": 0.1,
    }
    with pytest.raises(OracleConflict, match="knowledge precedes"):
        oracle_select([invalid_clock], "c-1", 10, 10)
    with pytest.raises(ValueError, match="unsupported"):
        PaymentEvent(
            "bad", "c-1", 10, 11, 1, "succeeded", 0.1, "bad-r3", "delete"  # type: ignore[arg-type]
        )


def test_dataset_exposes_requested_effective_and_truncated_cutoffs() -> None:
    data = _fixture()
    dataset = build_training_dataset(
        "g-stage1",
        (Label("label-1", "c-1", 200, 1, prediction_knowledge_time=180),),
        _definitions(data),
        _events(data),
        dataset_as_of=150,
    )
    row = dataset.rows[0]
    assert row["event_cutoff"] == 200
    assert row["prediction_knowledge_cutoff"] == 180
    assert row["dataset_as_of"] == 150
    assert row["knowledge_cutoff"] == 150
    assert row["knowledge_truncated_by_dataset_as_of"] is True
    assert dataset.manifest["row_cutoffs"] == [
        {
            "dataset_as_of": 150,
            "effective_knowledge_cutoff": 150,
            "event_cutoff": 200,
            "label_id": "label-1",
            "prediction_knowledge_cutoff": 180,
            "truncated_by_dataset_as_of": True,
        }
    ]


def test_default_policy_changes_definition_digest() -> None:
    definition = _definitions(_fixture())[2]
    assert definition.definition_digest != replace(
        definition, version=2, empty_default=0.5
    ).definition_digest
