from featureforge.simulator import run_failure_lab


def test_failure_lab_is_green_and_honest() -> None:
    result = run_failure_lab()
    assert result["result"] == "PASS"
    assert result["claim_level"] == "LOCAL_SIMULATION"
    assert result["production_claim"] is False
    assert result["metrics"]["checks_total"] >= 15
    assert result["metrics"]["checks_passed"] == result["metrics"]["checks_total"]
