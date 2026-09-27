"""Bounded local entry point for the verified Stage 3 Spark generation path."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.spark_runtime import create_local_spark, full_rebuild, write_generation


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    payload: dict[str, Any] = json.loads(args.input.read_text(encoding="utf-8"))
    required = {
        "generation_id",
        "definitions",
        "events",
        "customer_ids",
        "event_cutoff",
        "knowledge_cutoff",
    }
    if set(payload) != required:
        raise SystemExit(
            f"input shape mismatch: missing={sorted(required - set(payload))}, "
            f"unknown={sorted(set(payload) - required)}"
        )
    definitions = tuple(FeatureDefinition(**row) for row in payload["definitions"])
    events = tuple(PaymentEvent(**row) for row in payload["events"])
    spark = create_local_spark("featureforge-stage3-generation")
    try:
        build = full_rebuild(
            spark,
            payload["generation_id"],
            definitions,
            events,
            tuple(payload["customer_ids"]),
            event_cutoff=payload["event_cutoff"],
            knowledge_cutoff=payload["knowledge_cutoff"],
        )
        outcome = write_generation(build, args.output_root)
        print(f"{outcome}:{build.generation_id}:{build.manifest['rows_digest']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
