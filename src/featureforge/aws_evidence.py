"""Fail-closed contract for a future bounded AWS feature-platform lab."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from featureforge.canonical import digest

REQUIRED_RESOURCES = {
    "offline_bucket",
    "glue_database",
    "online_table",
    "state_machine_arn",
    "cloudwatch_log_group",
}
REQUIRED_FAILURES = {
    "late_correction",
    "future_leakage",
    "definition_drift",
    "parity_mismatch",
    "stale_publication",
}


def _mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _positive_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def validate_aws_lab_evidence(payload: Mapping[str, object]) -> tuple[str, ...]:
    """Return every reason a bundle cannot be classified as AWS-lab verified."""

    errors: list[str] = []
    expected_scalars = {
        "project": "featureforge-ml-feature-platform",
        "claim_level": "AWS_LAB_VERIFIED",
        "result": "PASS",
        "region": "ap-south-1",
    }
    for field, expected in expected_scalars.items():
        if payload.get(field) != expected:
            errors.append(f"{field} must equal {expected}")
    if payload.get("production_claim") is not False:
        errors.append("production_claim must remain false for a bounded lab")
    if not _nonempty_string(payload.get("run_id")) or len(str(payload.get("run_id"))) < 8:
        errors.append("run_id must be a stable non-empty identifier")
    if not _nonempty_string(payload.get("commit_sha")):
        errors.append("commit_sha is required")

    resources = _mapping(payload.get("resources"))
    if resources is None:
        errors.append("resources must be an object")
    else:
        missing = sorted(
            key for key in REQUIRED_RESOURCES if not _nonempty_string(resources.get(key))
        )
        if missing:
            errors.append(f"resources missing values: {', '.join(missing)}")

    failure_tests = payload.get("failure_tests")
    observed_failures = (
        {item for item in failure_tests if isinstance(item, str)}
        if isinstance(failure_tests, Sequence) and not isinstance(failure_tests, (str, bytes))
        else set()
    )
    missing_failures = sorted(REQUIRED_FAILURES - observed_failures)
    if missing_failures:
        errors.append(f"failure tests missing: {', '.join(missing_failures)}")

    metrics = _mapping(payload.get("metrics"))
    if metrics is None:
        errors.append("metrics must be an object")
    else:
        for field in ("feature_rows", "build_runtime_seconds", "serving_p95_ms"):
            if not _positive_number(metrics.get(field)):
                errors.append(f"metrics.{field} must be positive")
        cost = metrics.get("cost_usd")
        if not isinstance(cost, (int, float)) or isinstance(cost, bool) or cost < 0:
            errors.append("metrics.cost_usd must be a non-negative measured value")

    teardown = _mapping(payload.get("teardown"))
    if teardown is None or teardown.get("destroyed") is not True:
        errors.append("teardown.destroyed must be true")
    if teardown is None or not _nonempty_string(teardown.get("verified_at")):
        errors.append("teardown.verified_at is required")

    supplied_digest = payload.get("evidence_digest")
    unsigned = {key: value for key, value in payload.items() if key != "evidence_digest"}
    if supplied_digest != digest(unsigned):
        errors.append("evidence_digest does not match canonical payload")
    return tuple(errors)

