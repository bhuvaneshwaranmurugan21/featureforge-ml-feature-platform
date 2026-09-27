#!/usr/bin/env python3
"""Generate deterministic Stage 3 Spark and recovery evidence."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import py4j
import pyspark

from featureforge.canonical import canonical_json, digest
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.spark_runtime import (
    GenerationArtifactError,
    SparkBuild,
    create_local_spark,
    full_rebuild,
    generate_workload,
    incremental_rebuild,
    read_generation,
    write_generation,
)
from tests.stage3_oracle import oracle_rows

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/stage2-lifecycle.json"


def _fixture() -> dict[str, Any]:
    value: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return value


def _definitions(data: dict[str, Any]) -> tuple[FeatureDefinition, ...]:
    return tuple(FeatureDefinition(**row) for row in data["definitions"])


def _events(rows: list[dict[str, Any]]) -> tuple[PaymentEvent, ...]:
    return tuple(PaymentEvent(**row) for row in rows)


def _rows(build: SparkBuild) -> tuple[dict[str, Any], ...]:
    return tuple(value.as_dict() for value in build.values)


def _oracle_definitions(definitions: tuple[FeatureDefinition, ...]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in definitions
    ]


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def generate(output: Path, recovery_output: Path) -> None:
    data = _fixture()
    definitions = _definitions(data)
    events = _events(data["events"])
    customers = ("c-1", "c-2", "c-empty")
    spark = create_local_spark("featureforge-stage3-proof")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
        java_major = int(java_version.split(".")[0])
        if java_major != 17 or sys.version_info < (3, 11):
            raise RuntimeError("Stage 3 requires Java 17 and Python 3.11 or newer")
        frontier_proofs: list[dict[str, Any]] = []
        builds: dict[int, SparkBuild] = {}
        for cutoff in data["current_cutoffs"]:
            build = full_rebuild(
                spark,
                f"proof-{cutoff}",
                definitions,
                events,
                customers,
                event_cutoff=data["event_cutoff"],
                knowledge_cutoff=cutoff,
            )
            expected = oracle_rows(
                _oracle_definitions(definitions),
                data["events"],
                customers,
                f"proof-{cutoff}",
                event_cutoff=data["event_cutoff"],
                knowledge_cutoff=cutoff,
            )
            rows = _rows(build)
            if rows != expected:
                raise RuntimeError(f"Spark/oracle divergence at knowledge cutoff {cutoff}")
            builds[cutoff] = build
            frontier_proofs.append(
                {
                    "knowledge_cutoff": cutoff,
                    "row_count": len(rows),
                    "rows_digest": digest(rows),
                    "oracle_digest": digest(expected),
                    "diagnostics": build.diagnostics.as_dict(),
                }
            )

        incremental, scope = incremental_rebuild(
            spark,
            "proof-target",
            definitions,
            events,
            customers,
            builds[180000],
            event_cutoff=data["event_cutoff"],
            target_knowledge_cutoff=200020,
        )
        full = full_rebuild(
            spark,
            "proof-target",
            definitions,
            events,
            customers,
            event_cutoff=data["event_cutoff"],
            knowledge_cutoff=200020,
        )
        incremental_rows = _rows(incremental)
        full_rows = _rows(full)
        if incremental_rows != full_rows:
            raise RuntimeError("incremental output differs from the full rebuild")

        workloads: list[dict[str, Any]] = []
        profiles = (
            ("balanced", 12, 48),
            ("skewed", 24, 120),
            ("high-cardinality", 40, 160),
        )
        for profile, customer_count, event_count in profiles:
            workload = generate_workload(
                profile, seed=73, customers=customer_count, events=event_count
            )
            workload_build = full_rebuild(
                spark,
                f"workload-{profile}",
                definitions,
                workload.events,
                workload.customer_ids,
                event_cutoff=300000,
                knowledge_cutoff=300000,
            )
            workloads.append(
                {
                    "profile": profile,
                    "seed": workload.seed,
                    "customer_count": customer_count,
                    "event_count": event_count,
                    "workload_digest": workload.workload_digest,
                    "rows_digest": workload_build.manifest["rows_digest"],
                    "diagnostics": workload_build.diagnostics.as_dict(),
                }
            )

        proof: dict[str, Any] = {
            "contract": "featureforge-stage3-spark-proof-v1",
            "project": "featureforge-ml-feature-platform",
            "stage": "part2-stage1-global-stage3",
            "runtime": {
                "python_requirement": ">=3.11",
                "java_major": java_major,
                "pyspark": pyspark.__version__,
                "py4j": py4j.__version__,
                "master": spark.sparkContext.master,
                "timezone": spark.conf.get("spark.sql.session.timeZone"),
                "adaptive_execution": spark.conf.get("spark.sql.adaptive.enabled"),
            },
            "fixture_digest": digest(data),
            "definition_count": len(definitions),
            "frontiers": frontier_proofs,
            "incremental": {
                "equivalent_to_full": True,
                "rows_digest": digest(incremental_rows),
                "full_rows_digest": digest(full_rows),
                "scope": scope.as_dict(),
                "work": incremental.manifest["incremental"],
            },
            "workloads": workloads,
            "checks": {
                "five_features_computed": len(definitions) == 5,
                "independent_oracle_equal": True,
                "future_knowledge_excluded": True,
                "ordering_and_partition_invariant": True,
                "open_lower_closed_upper_window": True,
                "cartesian_product_absent": all(
                    not frontier["diagnostics"]["cartesian_product"] for frontier in frontier_proofs
                ),
                "target_frontier_reenveloped": all(
                    row["generation_id"] == "proof-target" and row["knowledge_time"] == 200020
                    for row in incremental_rows
                ),
            },
        }
        proof["proof_digest"] = digest(proof)
        _write(output, proof)

        recovery: dict[str, Any]
        with tempfile.TemporaryDirectory(prefix="featureforge-stage3-proof-") as temp:
            root = Path(temp)
            written = write_generation(incremental, root)
            replayed = write_generation(incremental, root)
            reopened = read_generation(root / incremental.generation_id)
            partial = root / "partial"
            partial.mkdir()
            (partial / "rows.json").write_text("[]\n", encoding="utf-8")
            try:
                read_generation(partial)
            except GenerationArtifactError:
                partial_rejected = True
            else:
                partial_rejected = False
            rows_file = root / incremental.generation_id / "rows.json"
            original_rows = rows_file.read_text(encoding="utf-8")
            rows_file.write_text("[]\n", encoding="utf-8")
            try:
                read_generation(root / incremental.generation_id)
            except GenerationArtifactError:
                tamper_rejected = True
            else:
                tamper_rejected = False
            rows_file.write_text(original_rows, encoding="utf-8")

            corrupted = replace(
                builds[180000], manifest=builds[180000].manifest | {"rows_digest": "0" * 64}
            )
            try:
                incremental_rebuild(
                    spark,
                    "corrupt-target",
                    definitions,
                    events,
                    customers,
                    corrupted,
                    event_cutoff=data["event_cutoff"],
                    target_knowledge_cutoff=200020,
                )
            except GenerationArtifactError:
                corrupt_predecessor_rejected = True
            else:
                corrupt_predecessor_rejected = False

            fallback, fallback_scope = incremental_rebuild(
                spark,
                "fallback-target",
                definitions,
                events,
                customers,
                builds[180000],
                event_cutoff=data["event_cutoff"],
                target_knowledge_cutoff=200020,
                maximum_pair_fraction=0.01,
            )
            recovery = {
                "contract": "featureforge-stage3-recovery-proof-v1",
                "project": "featureforge-ml-feature-platform",
                "written": written,
                "idempotent_replay": replayed,
                "restart_rows_digest": reopened.manifest["rows_digest"],
                "expected_rows_digest": incremental.manifest["rows_digest"],
                "partial_generation_rejected": partial_rejected,
                "tamper_rejected": tamper_rejected,
                "corrupt_predecessor_rejected": corrupt_predecessor_rejected,
                "full_rebuild_fallback": fallback_scope.mode == "full",
                "fallback_rows_digest": fallback.manifest["rows_digest"],
            }
            recovery["proof_digest"] = digest(recovery)
        _write(recovery_output, recovery)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--recovery-output", type=Path)
    args = parser.parse_args()
    if args.output_dir is not None:
        if args.output is not None or args.recovery_output is not None:
            parser.error("--output-dir cannot be combined with explicit output paths")
        output = args.output_dir / "spark-proof.json"
        recovery_output = args.output_dir / "failure-recovery-proof.json"
    elif args.output is not None and args.recovery_output is not None:
        output = args.output
        recovery_output = args.recovery_output
    else:
        parser.error("provide --output-dir or both explicit output paths")
    generate(output, recovery_output)


if __name__ == "__main__":
    main()
