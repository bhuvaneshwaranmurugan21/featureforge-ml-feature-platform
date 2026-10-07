"""Pure golden/negative tests; unit fixtures are never live AWS evidence."""

import json
from pathlib import Path

import pytest

from featureforge.canonical import digest
from featureforge.stage6_live import LiveEvidenceError
from tools import reconstitute_stage6_historical as historical


def test_exact_original_normalized_golden() -> None:
    root = Path(__file__).resolve().parents[1]
    result = historical.normalize_source(historical.pinned_files(root))
    assert result["normalized_plan_sha256"] == historical.ORIGINAL_SHAS["NORMALIZED_PLAN_SHA256"]
    assert result["resource_count"] == 42


def test_exact_original_no_mutation_golden() -> None:
    value = historical.no_mutation_receipt()
    checksum = value.pop("receipt_sha256")
    assert digest(value) == checksum == historical.ORIGINAL_SHAS["NO_MUTATION_RECEIPT_SHA256"]


def test_original_authority_uses_authenticated_historical_model() -> None:
    root = Path(__file__).resolve().parents[1]
    collector = json.loads((root / "evidence/stage6/recovery-observation.json").read_text())
    model = historical.historical_authority_type(root)
    assert model.__module__ == "_featureforge_stage6_authenticated_historical_managed"
    value = historical.authority_receipt(collector, model)
    checksum = value.pop("authority_receipt_sha256")
    assert digest(value) == checksum == historical.ORIGINAL_SHAS["PLAN_AUTHORITY_RECEIPT_SHA256"]


@pytest.mark.parametrize("text", ["", 'resource "aws_s3_bucket" "unsafe" {}'])
def test_incomplete_or_changed_graph_rejected(text: str) -> None:
    with pytest.raises(LiveEvidenceError):
        historical.normalize_source({"main.tf": text})


def test_changed_normalized_identity_rejected() -> None:
    root = Path(__file__).resolve().parents[1]
    files = historical.pinned_files(root)
    files["main.tf"] = files["main.tf"].replace('"aws_kms_key" "platform"',
                                              '"aws_kms_key" "wrong"')
    with pytest.raises(LiveEvidenceError, match="exact original digest"):
        historical.normalize_source(files)


def test_tampered_collector_checksum_rejected() -> None:
    with pytest.raises(LiveEvidenceError, match="digest mismatch"):
        historical.verify_collector({"receipt_sha256": "0" * 64})


def test_missing_and_duplicate_digest_witness_rejected() -> None:
    with pytest.raises(LiveEvidenceError, match="digest witness"):
        historical.verify_witness("")
    key = "BINARY_PLAN_SHA256"
    line = f"{key}={historical.ORIGINAL_SHAS[key]}\n"
    with pytest.raises(LiveEvidenceError, match="duplicate"):
        historical.verify_witness(line * 2)
