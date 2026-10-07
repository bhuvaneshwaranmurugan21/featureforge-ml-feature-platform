from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from tests.stage6_oracle import admission_projection, cost_projection
from tools import run_stage6_proof


def test_primitive_cost_uses_each_rate_and_rounds_up() -> None:
    raw = {
        "pricing_observed_at_epoch": 1,
        "lines": [{"component": "fractional", "quantity_millionths": 1, "unit_cost_microusd": 1}],
        "safety_margin_bps": 2_000,
        "maximum_microusd": 2,
    }
    assert cost_projection(raw)["subtotal_microusd"] == 1
    assert cost_projection(raw)["worst_case_microusd"] == 2
    assert cost_projection(raw)["admitted"] is True


@pytest.mark.parametrize("mutation", ["account", "region", "lease", "inventory", "quota", "cost"])
def test_independent_oracle_denies_all_existing_admission_classes(mutation: str) -> None:
    raw = deepcopy(run_stage6_proof.primitive_fixture())
    if mutation == "account":
        raw["observed_account_fingerprint"] = "f" * 64
    elif mutation == "region":
        raw["observed_region"] = "eu-west-1"
    elif mutation == "lease":
        raw["observed_at_epoch"] = 1_600
    elif mutation == "inventory":
        raw["inventory"]["items"] = ["featureforge-stage6-old"]
    elif mutation == "quota":
        raw["available_quotas"]["glue-concurrent-runs"] = 0
    else:
        raw["cost"]["lines"][0]["unit_cost_microusd"] = 30_000_000
    assert admission_projection(raw)["decision"] == "DENIED"


def test_proof_fails_when_production_admission_digest_disagrees(monkeypatch: Any) -> None:
    original = run_stage6_proof.admit_managed_run

    def corrupted(*args: Any, **kwargs: Any) -> Any:
        return replace(original(*args, **kwargs), cost_digest="f" * 64)

    monkeypatch.setattr(run_stage6_proof, "admit_managed_run", corrupted)
    with pytest.raises(AssertionError, match="independent oracle disagrees"):
        run_stage6_proof.proofs()


def test_proof_preserves_all_denial_controls_and_computes_oracle_checks() -> None:
    local, failure = run_stage6_proof.proofs()
    assert local["oracle_comparisons"] == {"manifest": True, "cost": True, "admission": True}
    assert len(failure["admission_controls"]) == 6
    assert len(failure["stale_plan_controls"]) == 14
    assert all(row["oracle_outcome"] == "DENIED" for row in failure["admission_controls"])
