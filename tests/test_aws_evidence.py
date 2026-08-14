from featureforge.aws_evidence import validate_aws_lab_evidence
from featureforge.canonical import digest


def valid_bundle() -> dict[str, object]:
    payload: dict[str, object] = {
        "project": "featureforge-ml-feature-platform",
        "claim_level": "AWS_LAB_VERIFIED",
        "production_claim": False,
        "result": "PASS",
        "region": "ap-south-1",
        "run_id": "ff-20260814-001",
        "commit_sha": "0123456789abcdef",
        "resources": {
            "offline_bucket": "featureforge-redacted-offline",
            "glue_database": "featureforge_lab",
            "online_table": "featureforge-lab-online",
            "state_machine_arn": "redacted:state-machine",
            "cloudwatch_log_group": "/aws/featureforge/redacted",
        },
        "failure_tests": [
            "late_correction",
            "future_leakage",
            "definition_drift",
            "parity_mismatch",
            "stale_publication",
        ],
        "metrics": {
            "feature_rows": 1000,
            "build_runtime_seconds": 38.5,
            "serving_p95_ms": 12.3,
            "cost_usd": 0.95,
        },
        "teardown": {"destroyed": True, "verified_at": "2026-08-14T12:00:00Z"},
    }
    payload["evidence_digest"] = digest(payload)
    return payload


def test_complete_aws_evidence_contract_passes() -> None:
    assert validate_aws_lab_evidence(valid_bundle()) == ()


def test_evidence_contract_fails_closed_after_mutation() -> None:
    payload = valid_bundle()
    payload["failure_tests"] = ["late_correction"]
    payload["metrics"] = {"feature_rows": 0}
    errors = validate_aws_lab_evidence(payload)
    assert any("failure tests missing" in error for error in errors)
    assert any("build_runtime_seconds" in error for error in errors)
    assert any("evidence_digest" in error for error in errors)

