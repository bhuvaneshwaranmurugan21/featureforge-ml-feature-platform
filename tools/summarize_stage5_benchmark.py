#!/usr/bin/env python3
"""Derive the deterministic Stage 5 benchmark summary from immutable raw trials."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from featureforge.assurance import BenchmarkTrial, summarize_trials
from featureforge.canonical import canonical_json, digest


def summarize(raw_path: Path, output: Path) -> None:
    raw: dict[str, Any] = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_body = dict(raw)
    recorded = raw_body.pop("raw_digest", None)
    if recorded != digest(raw_body):
        raise RuntimeError("raw benchmark digest is invalid")
    trials: list[BenchmarkTrial] = []
    for value in raw.get("trials", []):
        body = dict(value)
        trial_digest = body.pop("trial_digest", None)
        contract = body.pop("contract", None)
        if contract != "stage5-benchmark-trial-v1" or trial_digest != digest(
            {"contract": contract, **body}
        ):
            raise RuntimeError("benchmark trial is invalid")
        trials.append(BenchmarkTrial(**body))
    summary = summarize_trials(trials)
    payload: dict[str, Any] = {
        "case_count": len(raw.get("cases", [])),
        "contract": "featureforge-stage5-benchmark-report-v1",
        "environment": raw.get("environment"),
        "profile_count": len({row.profile for row in trials}),
        "project": "featureforge-ml-feature-platform",
        "protocol_digest": raw.get("protocol_digest"),
        "raw_digest": recorded,
        "size_class_count": len({row.size_class for row in trials}),
        "stage": "part2-stage3-global-stage5",
        "summary": summary,
        "trial_count": len(trials),
    }
    payload["report_digest"] = digest(payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    summarize(args.raw, args.output)
    print(f"wrote Stage 5 benchmark summary to {args.output}")


if __name__ == "__main__":
    main()
