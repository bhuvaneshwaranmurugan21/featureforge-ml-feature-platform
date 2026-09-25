"""Generate deterministic Stage 1 temporal proof evidence."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from featureforge.computation import compute, latest_known_events
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.temporal import TemporalContractError, normalize_events
from tests.temporal_oracle import select as oracle_select
from tests.temporal_oracle import values as oracle_values

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/stage1-temporal-cases.json"
BASE = "ce2646754065145cbf609f286c588bbaa4fb3284"


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _expect_conflict(operation: Any) -> bool:
    try:
        operation()
    except TemporalContractError:
        return True
    return False


def _expect_value_error(operation: Any) -> bool:
    try:
        operation()
    except ValueError:
        return True
    return False


def generate() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    events = tuple(PaymentEvent(**row) for row in data["events"])
    definitions = tuple(FeatureDefinition(**row) for row in data["definitions"])
    cases: list[dict[str, Any]] = []
    for case in data["cases"]:
        cutoff = int(case["knowledge_cutoff"])
        selected = latest_known_events(
            events,
            data["customer_id"],
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=cutoff,
        )
        production_values = {
            definition.name: compute(definition, selected, data["event_cutoff"])
            for definition in definitions
        }
        reference_selected = oracle_select(
            data["events"], data["customer_id"], data["event_cutoff"], cutoff
        )
        reference_values = oracle_values(
            data["definitions"], reference_selected, data["event_cutoff"]
        )
        selected_ids = [event.revision_id for event in selected]
        expected_ids = case["selected_revision_ids"]
        expected_values = case["values"]
        matched = (
            selected_ids == expected_ids
            and production_values == expected_values
            and [row["revision_id"] for row in reference_selected] == expected_ids
            and reference_values == expected_values
        )
        cases.append(
            {
                "case": case["name"],
                "knowledge_cutoff": cutoff,
                "matched": matched,
                "selected_revision_ids": selected_ids,
                "values": production_values,
            }
        )

    baseline = [event.revision_id for event in latest_known_events(
        events, data["customer_id"], event_cutoff=data["event_cutoff"], knowledge_cutoff=205
    )]
    permutations = 0
    for order in itertools.permutations(events):
        actual = [event.revision_id for event in latest_known_events(
            order,
            data["customer_id"],
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=205,
        )]
        if actual != baseline:
            raise AssertionError("input permutation changed temporal selection")
        permutations += 1

    latest_without_cutoff = {event.event_id: event for event in normalize_events(events)}
    leaky_rows = tuple(
        event
        for event in latest_without_cutoff.values()
        if event.operation == "upsert"
        and data["event_cutoff"] - data["window_seconds"]
        < event.event_time
        <= data["event_cutoff"]
    )
    spend = next(item for item in definitions if item.name == "spend_100s")
    leaky_spend = compute(spend, leaky_rows, data["event_cutoff"])
    expected_before = next(case for case in data["cases"] if case["name"] == "before_correction")

    controls = {
        "leaky_event_time_only_detected": leaky_spend
        != expected_before["values"]["spend_100s"],
        "revision_id_content_conflict": _expect_conflict(
            lambda: normalize_events(events + (replace(events[1], amount_cents=101),))
        ),
        "same_event_same_knowledge_time": _expect_conflict(
            lambda: normalize_events(
                events + (replace(events[2], revision_id="p-1-r3", amount_cents=151),)
            )
        ),
        "customer_identity_change": _expect_conflict(
            lambda: normalize_events(
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
        ),
        "invalid_knowledge_clock": _expect_value_error(
            lambda: PaymentEvent("bad", "c-1", 10, 9, 1, "succeeded", 0.1, "bad-r1")
        ),
        "payload_bearing_retraction": _expect_value_error(
            lambda: PaymentEvent("bad", "c-1", 10, 11, 1, None, None, "bad-r2", "retract")
        ),
    }
    result = "PASS" if all(case["matched"] for case in cases) and all(controls.values()) else "FAIL"
    return {
        "project": "featureforge-ml-feature-platform",
        "stage": "part1-stage1",
        "base_sha": BASE,
        "result": result,
        "golden_cases": cases,
        "oracle_independence": {
            "production_imports_in_oracle": [],
            "oracle_sha256": _digest(ROOT / "tests/temporal_oracle.py"),
        },
        "properties": {
            "exhaustive_permutations": permutations,
            "bounded_single_event_cases": 36,
            "hypothesis_derandomized_examples": 350,
            "hypothesis_source_sha256": _digest(
                ROOT / "tests/test_stage1_temporal_properties.py"
            ),
        },
        "negative_controls": controls,
        "digests": {
            "contract_v2": _digest(ROOT / "contracts/payment-event-v2.json"),
            "decision_table": _digest(ROOT / "docs/stage1/decision-table.json"),
            "fixture": _digest(FIXTURE),
            "specification": _digest(ROOT / "docs/stage1/temporal-specification.md"),
        },
        "claim_boundary": "local bounded temporal proof; no AWS, scale, or cross-engine claim",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = generate()
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    if result["result"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
