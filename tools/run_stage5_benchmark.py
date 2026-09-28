#!/usr/bin/env python3
"""Execute the frozen bounded Stage 5 benchmark and retain every raw trial."""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, replace
from functools import partial
from pathlib import Path
from typing import Any, TypeVar

import py4j
import pyspark

from featureforge.assurance import (
    ActivationGateInputs,
    BenchmarkTrial,
    activate_eligible,
    evaluate_activation,
)
from featureforge.canonical import canonical_json, digest
from featureforge.expected import (
    compare_online_records,
    project_feature_rows,
    project_online_records,
)
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.online import LocalOnlineStore, build_materialization_plan
from featureforge.spark_runtime import (
    create_local_spark,
    full_rebuild,
    generate_workload,
    incremental_rebuild,
    plan_affected_scope,
    read_generation,
    validate_inputs,
    write_generation,
)
from tests.stage2_support import definitions, fixture

T = TypeVar("T")
PROJECT = "featureforge-ml-feature-platform"
FEATURE_SET = "payment-risk-v1"
SEED = 73
EVENT_CUTOFF = 300000
BASE_CUTOFF = 300000
TARGET_CUTOFF = 300001
SOURCE_DIGEST = "a" * 64
SIZES = (("small", 4, 12), ("medium", 8, 24), ("large", 12, 48))
PROFILES = ("balanced", "skewed", "high-cardinality")


def _timed(call: Callable[[], T]) -> tuple[int, T]:
    started = time.perf_counter_ns()
    value = call()
    elapsed = time.perf_counter_ns() - started
    if elapsed <= 0:
        raise RuntimeError("monotonic timer returned a non-positive duration")
    return elapsed, value


def _definition_rows(rows: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [asdict(row) | {"definition_digest": row.definition_digest} for row in rows]


def _primitive_events(rows: tuple[PaymentEvent, ...]) -> list[dict[str, Any]]:
    return [row.as_dict() for row in rows]


def _corrected_events(
    events: tuple[PaymentEvent, ...], profile: str
) -> tuple[PaymentEvent, ...]:
    correction_count = {
        "balanced": 1,
        "skewed": max(1, len(events) // 4),
        "high-cardinality": len(events),
    }[profile]
    corrections = tuple(
        replace(
            row,
            revision_id=f"{row.event_id}-r2",
            knowledge_time=TARGET_CUTOFF,
            amount_cents=row.amount_cents + 1,
        )
        for row in events[:correction_count]
    )
    return (*events, *corrections)


def _record(
    rows: list[BenchmarkTrial],
    *,
    case_id: str,
    operation: str,
    profile: str,
    size_class: str,
    trial: int,
    duration_ns: int,
    input_digest: str,
    output_digest: str,
) -> None:
    rows.append(
        BenchmarkTrial(
            case_id=case_id,
            operation=operation,
            profile=profile,
            size_class=size_class,
            trial=trial,
            warm=trial > 0,
            duration_ns=duration_ns,
            input_digest=input_digest,
            output_digest=output_digest,
            correctness_passed=True,
        )
    )


def _artifact_roundtrip(target: Any, root: Path, generation_id: str) -> Any:
    write_generation(target, root)
    return read_generation(root / generation_id)


def _read_all(store: Any, token: Any, records: tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(
        store.read(
            token,
            row.customer_id,
            row.feature_name,
            definition_digest=row.definition_digest,
            request_time=TARGET_CUTOFF + 3,
        ).status
        for row in records
    )


def run(output: Path, protocol: Path) -> None:
    protocol_value: dict[str, Any] = json.loads(protocol.read_text(encoding="utf-8"))
    if protocol_value.get("status") != "frozen-before-publishable-measurement":
        raise RuntimeError("benchmark protocol is not frozen")
    defs = definitions(fixture())
    definition_rows = _definition_rows(defs)
    spark = create_local_spark("featureforge-stage5-benchmark", shuffle_partitions=4)
    spark.sparkContext.setLogLevel("ERROR")
    trials: list[BenchmarkTrial] = []
    cases: list[dict[str, Any]] = []
    try:
        for size_class, customer_count, event_count in SIZES:
            for profile in PROFILES:
                case_id = f"{size_class}-{profile}"
                workload = generate_workload(
                    profile, seed=SEED, customers=customer_count, events=event_count
                )
                events = _corrected_events(workload.events, profile)
                input_digest = digest(_primitive_events(events))
                generation_id = f"bench-{case_id}"
                base = full_rebuild(
                    spark,
                    f"base-{case_id}",
                    defs,
                    events,
                    workload.customer_ids,
                    event_cutoff=EVENT_CUTOFF,
                    knowledge_cutoff=BASE_CUTOFF,
                )
                full_target = full_rebuild(
                    spark,
                    generation_id,
                    defs,
                    events,
                    workload.customer_ids,
                    event_cutoff=EVENT_CUTOFF,
                    knowledge_cutoff=TARGET_CUTOFF,
                )
                maximum_fraction = 0.05 if profile == "high-cardinality" else 0.75
                incremental_target, scope = incremental_rebuild(
                    spark,
                    generation_id,
                    defs,
                    events,
                    workload.customer_ids,
                    base,
                    event_cutoff=EVENT_CUTOFF,
                    target_knowledge_cutoff=TARGET_CUTOFF,
                    maximum_pair_fraction=maximum_fraction,
                )
                full_rows = tuple(row.as_dict() for row in full_target.values)
                incremental_rows = tuple(row.as_dict() for row in incremental_target.values)
                if full_rows != incremental_rows:
                    raise RuntimeError(f"full/incremental mismatch for {case_id}")
                expected_features = project_feature_rows(
                    definition_rows,
                    _primitive_events(events),
                    workload.customer_ids,
                    generation_id,
                    event_cutoff=EVENT_CUTOFF,
                    knowledge_cutoff=TARGET_CUTOFF,
                )
                if expected_features != full_rows:
                    raise RuntimeError(f"independent projector mismatch for {case_id}")
                online_plan = build_materialization_plan(
                    FEATURE_SET,
                    f"materialize-{case_id}",
                    defs,
                    full_target.values,
                    source_digest=input_digest,
                    materialized_at=TARGET_CUTOFF + 1,
                )
                expected_online = project_online_records(
                    definition_rows,
                    expected_features,
                    feature_set=FEATURE_SET,
                    operation_id=online_plan.operation_id,
                    source_digest=input_digest,
                    materialized_at=TARGET_CUTOFF + 1,
                )
                parity = compare_online_records(
                    expected_online, tuple(row.as_dict() for row in online_plan.records)
                )
                if not parity["passed"]:
                    raise RuntimeError(f"online parity mismatch for {case_id}")
                output_digest = digest(full_rows)
                cases.append(
                    {
                        "affected_pair_count": len(scope.affected_pairs),
                        "case_id": case_id,
                        "customer_count": customer_count,
                        "diagnostics": full_target.diagnostics.as_dict(),
                        "event_count": event_count,
                        "incremental_mode": scope.mode,
                        "input_digest": input_digest,
                        "maximum_pair_fraction": maximum_fraction,
                        "output_digest": output_digest,
                        "profile": profile,
                        "size_class": size_class,
                    }
                )

                for trial in range(6):
                    duration, generated = _timed(
                        partial(
                            generate_workload,
                            profile,
                            seed=SEED,
                            customers=customer_count,
                            events=event_count,
                        )
                    )
                    validate_inputs(
                        defs,
                        generated.events,
                        generated.customer_ids,
                        event_cutoff=EVENT_CUTOFF,
                        knowledge_cutoff=BASE_CUTOFF,
                    )
                    _record(
                        trials,
                        case_id=case_id,
                        operation="workload_input_validation",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=workload.workload_digest,
                        output_digest=generated.workload_digest,
                    )

                    duration, rebuilt = _timed(
                        partial(
                            full_rebuild,
                            spark,
                            generation_id,
                            defs,
                            events,
                            workload.customer_ids,
                            event_cutoff=EVENT_CUTOFF,
                            knowledge_cutoff=TARGET_CUTOFF,
                        )
                    )
                    rebuilt_digest = digest(tuple(row.as_dict() for row in rebuilt.values))
                    if rebuilt_digest != output_digest:
                        raise RuntimeError("timed full rebuild correctness drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="spark_full_rebuild",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=input_digest,
                        output_digest=rebuilt_digest,
                    )

                    duration, planned = _timed(
                        partial(
                            plan_affected_scope,
                            defs,
                            events,
                            event_cutoff=EVENT_CUTOFF,
                            base_knowledge_cutoff=BASE_CUTOFF,
                            target_knowledge_cutoff=TARGET_CUTOFF,
                            maximum_pair_fraction=maximum_fraction,
                        )
                    )
                    if planned.plan_digest != scope.plan_digest:
                        raise RuntimeError("timed affected-scope plan drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="affected_scope_planning",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=input_digest,
                        output_digest=planned.plan_digest,
                    )

                    duration, rebuilt_pair = _timed(
                        partial(
                            incremental_rebuild,
                            spark,
                            generation_id,
                            defs,
                            events,
                            workload.customer_ids,
                            base,
                            event_cutoff=EVENT_CUTOFF,
                            target_knowledge_cutoff=TARGET_CUTOFF,
                            maximum_pair_fraction=maximum_fraction,
                        )
                    )
                    rebuilt_incremental, measured_scope = rebuilt_pair
                    incremental_digest = digest(
                        tuple(row.as_dict() for row in rebuilt_incremental.values)
                    )
                    if incremental_digest != output_digest or measured_scope != scope:
                        raise RuntimeError("timed incremental rebuild correctness drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="spark_incremental_rebuild",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=input_digest,
                        output_digest=incremental_digest,
                    )

                    with tempfile.TemporaryDirectory(prefix="ff-stage5-artifact-") as temp:
                        artifact_root = Path(temp)
                        duration, reopened = _timed(
                            partial(
                                _artifact_roundtrip,
                                full_target,
                                artifact_root,
                                generation_id,
                            )
                        )
                        artifact_digest = digest(
                            tuple(row.as_dict() for row in reopened.values)
                        )
                    if artifact_digest != output_digest:
                        raise RuntimeError("artifact roundtrip correctness drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="artifact_roundtrip",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=output_digest,
                        output_digest=artifact_digest,
                    )

                    with tempfile.TemporaryDirectory(prefix="ff-stage5-online-") as temp:
                        store = LocalOnlineStore(Path(temp) / "online.sqlite")
                        duration, receipt = _timed(partial(store.materialize, online_plan))
                        store.close()
                    _record(
                        trials,
                        case_id=case_id,
                        operation="online_materialize_reconcile",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=online_plan.plan_digest,
                        output_digest=receipt["receipt_digest"],
                    )

                    duration, measured_parity = _timed(
                        partial(
                            compare_online_records,
                            expected_online,
                            tuple(row.as_dict() for row in online_plan.records),
                        )
                    )
                    if not measured_parity["passed"]:
                        raise RuntimeError("timed parity correctness drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="independent_parity",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=parity["expected_digest"],
                        output_digest=measured_parity["report_digest"],
                    )

                    with tempfile.TemporaryDirectory(prefix="ff-stage5-activation-") as temp:
                        store = LocalOnlineStore(Path(temp) / "online.sqlite")
                        receipt = store.materialize(online_plan)
                        inputs = ActivationGateInputs(
                            feature_set=FEATURE_SET,
                            generation_id=generation_id,
                            operation_id=f"activate-{case_id}",
                            validation_receipt_digest=receipt["receipt_digest"],
                            expected_generation=None,
                            expected_version=0,
                            evaluated_at=TARGET_CUTOFF + 2,
                            evidence_digests=(parity["report_digest"], receipt["receipt_digest"]),
                            source_complete=True,
                            manifests_valid=True,
                            offline_valid=True,
                            online_reconciled=True,
                            parity_passed=True,
                            freshness_passed=True,
                            candidate_consistent=True,
                            unique_candidate=True,
                            pointer_current=True,
                        )
                        duration, activation = _timed(
                            partial(
                                activate_eligible,
                                store,
                                evaluate_activation(inputs),
                                actor="stage5-benchmark",
                                reason="benchmark-correctness-gate",
                            )
                        )
                        token = store.pin_reader(FEATURE_SET)
                        duration_read, read_results = _timed(
                            partial(_read_all, store, token, online_plan.records)
                        )
                        store.close()
                    if any(status != "FOUND" for status in read_results):
                        raise RuntimeError("generation-pinned read correctness drift")
                    _record(
                        trials,
                        case_id=case_id,
                        operation="activation_decision_cas",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration,
                        input_digest=receipt["receipt_digest"],
                        output_digest=activation["receipt_digest"],
                    )
                    _record(
                        trials,
                        case_id=case_id,
                        operation="generation_pinned_reads",
                        profile=profile,
                        size_class=size_class,
                        trial=trial,
                        duration_ns=duration_read,
                        input_digest=activation["receipt_digest"],
                        output_digest=digest(read_results),
                    )
    finally:
        spark.stop()

    payload: dict[str, Any] = {
        "cases": cases,
        "contract": "featureforge-stage5-benchmark-raw-v1",
        "environment": {
            "cpu_count": os.cpu_count(),
            "java_major": 17,
            "machine": platform.machine(),
            "maximum_resident_set_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "operating_system": platform.platform(),
            "py4j": py4j.__version__,
            "pyspark": pyspark.__version__,
            "python": sys.version.split()[0],
            "spark_master": "local[2]",
        },
        "project": PROJECT,
        "protocol_digest": digest(protocol_value),
        "stage": "part2-stage3-global-stage5",
        "trial_count": len(trials),
        "trials": [row.as_dict() for row in trials],
    }
    payload["raw_digest"] = digest(payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--protocol",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs/stage5/benchmark-protocol.json",
    )
    args = parser.parse_args()
    run(args.output, args.protocol)
    print(f"wrote Stage 5 raw benchmark evidence to {args.output}")


if __name__ == "__main__":
    main()
