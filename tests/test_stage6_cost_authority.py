from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.session import get_session

from featureforge.stage6_live import (
    LiveEvidenceError,
    budget_headroom,
    ondemand_dimensions,
    priced_cost_envelope,
    usd_microusd,
)
from tools import qualify_stage6_aws as qualifier


def product(rate: str = "0.0000001", region: str = "ap-southeast-2") -> dict[str, Any]:
    return {
        "publicationDate": "2026-01-01T00:00:00Z",
        "product": {
            "sku": "sku",
            "attributes": {"regionCode": region, "usagetype": "Lambda-Requests"},
        },
        "terms": {
            "OnDemand": {
                "term": {
                    "effectiveDate": "2026-01-01T00:00:00Z",
                    "priceDimensions": {
                        "dimension": {
                            "unit": "Requests",
                            "beginRange": "0",
                            "endRange": "Inf",
                            "pricePerUnit": {"USD": rate},
                        }
                    },
                }
            }
        },
    }


def assert_closed_cost_schema(value: Any, schema: dict[str, Any]) -> None:
    """Independently check every constraint used by the frozen cost-only schema."""
    supported = {
        "$schema",
        "$id",
        "type",
        "additionalProperties",
        "required",
        "properties",
        "const",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "minItems",
        "items",
    }
    assert not set(schema) - supported
    if "const" in schema:
        assert value == schema["const"]
    kind = schema.get("type")
    if kind == "object":
        assert type(value) is dict
        assert set(schema["required"]) <= set(value)
        assert schema["additionalProperties"] is False
        assert not set(value) - set(schema["properties"])
        for key, child in value.items():
            assert_closed_cost_schema(child, schema["properties"][key])
    elif kind == "array":
        assert type(value) is list
        assert len(value) >= schema["minItems"]
        for item in value:
            assert_closed_cost_schema(item, schema["items"])
    elif kind == "integer":
        assert type(value) is int
        assert value >= schema.get("minimum", value)
        assert value <= schema.get("maximum", value)
    elif kind == "string":
        assert type(value) is str
        assert len(value) >= schema.get("minLength", len(value))
        assert len(value) <= schema.get("maxLength", len(value))
    elif kind == "boolean":
        assert type(value) is bool
    else:
        assert kind is None and "const" in schema


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-1", "garbage"])
def test_money_rejects_nonfinite_negative_or_invalid(amount: str) -> None:
    with pytest.raises(LiveEvidenceError):
        usd_microusd(amount)


def test_price_requires_region_currency_effective_date_and_provenance() -> None:
    row = ondemand_dimensions(product(), region="ap-southeast-2", observed_at_epoch=1_800_000_000)[
        0
    ]
    assert row["unit_cost_microusd"] == 1
    assert row["rate_id"] == "dimension"
    assert len(row["catalog_sha256"]) == 64
    with pytest.raises(LiveEvidenceError, match="region"):
        ondemand_dimensions(
            product(region="us-east-1"), region="ap-southeast-2", observed_at_epoch=1_800_000_000
        )
    assert not ondemand_dimensions(product(), region="ap-southeast-2", observed_at_epoch=100)
    malformed = product()
    malformed["terms"]["OnDemand"]["term"]["priceDimensions"]["dimension"]["pricePerUnit"] = {
        "EUR": "1"
    }
    with pytest.raises(LiveEvidenceError, match="USD"):
        ondemand_dimensions(malformed, region="ap-southeast-2", observed_at_epoch=1_800_000_000)


def test_live_prices_paginate_and_choose_maximum_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    class Catalog:
        calls: list[dict[str, Any]] = []

        def get_products(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            if "NextToken" not in kwargs:
                return {"PriceList": [json.dumps(product("0"))], "NextToken": "next"}
            return {"PriceList": [json.dumps(product("0.0000003"))]}

    catalog = Catalog()
    monkeypatch.setattr(qualifier, "PRICE_SERVICES", ("AWSLambda",))
    monkeypatch.setattr(qualifier, "_client", lambda *args: catalog)
    profile = {
        "lines": [
            {
                "component": "requests",
                "service_code": "AWSLambda",
                "units": ["Requests"],
                "usage_pattern": "Lambda-Requests",
            }
        ]
    }
    result = qualifier._pricing(1_800_000_000, profile)
    assert len(catalog.calls) == 2
    assert catalog.calls[1]["NextToken"] == "next"
    assert result[0]["matching_dimension_count"] == 2
    assert result[0]["unit_cost_microusd"] == 1
    assert result[0]["selected_dimension"]["rate_id"] == "dimension"


def test_global_dashboard_price_has_explicit_narrow_applicability() -> None:
    observed = product("3", region="")
    observed["product"]["attributes"] |= {
        "servicecode": "AmazonCloudWatch",
        "location": "Any",
        "usagetype": "DashboardsUsageHour",
    }
    dimension = observed["terms"]["OnDemand"]["term"]["priceDimensions"]["dimension"]
    dimension["unit"] = "Dashboards"
    with pytest.raises(LiveEvidenceError, match="region"):
        ondemand_dimensions(observed, region="ap-southeast-2", observed_at_epoch=1_800_000_000)
    rows = ondemand_dimensions(
        observed,
        region="ap-southeast-2",
        observed_at_epoch=1_800_000_000,
        allow_global_dashboard=True,
    )
    assert rows[0]["pricing_scope"] == "ACCOUNT_GLOBAL_DASHBOARD"
    assert rows[0]["unit"] == "Dashboards"
    assert rows[0]["unit_cost_microusd"] == 3_000_000
    assert rows[0]["attributes"]["regionCode"] == ""
    for field, value in (
        ("location", "US East (N. Virginia)"),
        ("regionCode", "us-east-1"),
        ("servicecode", "AWSLambda"),
        ("usagetype", "Global-DashboardsUsageHour-Basic"),
    ):
        wrong = json.loads(json.dumps(observed))
        wrong["product"]["attributes"][field] = value
        with pytest.raises(LiveEvidenceError, match="region"):
            ondemand_dimensions(
                wrong,
                region="ap-southeast-2",
                observed_at_epoch=1_800_000_000,
                allow_global_dashboard=True,
            )


def test_cost_arithmetic_conforms_to_closed_contract_and_does_not_hide_missing_lines() -> None:
    profile = {"lines": [{"component": "requests", "quantity_millionths": 3}]}
    rates = [{"component": "requests", "unit_cost_microusd": 1}]
    result = priced_cost_envelope(profile, rates, observed_at_epoch=100)
    assert_closed_cost_schema(
        result, json.loads(Path("contracts/stage6-cost-envelope-v1.json").read_text())
    )
    assert result["subtotal_microusd"] == 1
    assert result["worst_case_microusd"] == 2
    with pytest.raises(LiveEvidenceError, match="coverage"):
        priced_cost_envelope(profile, [], observed_at_epoch=100)
    with pytest.raises(LiveEvidenceError, match="coverage"):
        priced_cost_envelope(profile, rates + rates, observed_at_epoch=100)
    rates[0]["unit_cost_microusd"] = 100_000_000_000_000
    with pytest.raises(LiveEvidenceError, match="USD 25"):
        priced_cost_envelope(profile, rates, observed_at_epoch=100)


def budget(limit: str = "100", actual: str = "10", forecast: str = "80") -> dict[str, Any]:
    return {
        "BudgetName": "private-account-budget",
        "BudgetType": "COST",
        "TimeUnit": "MONTHLY",
        "BudgetLimit": {"Amount": limit, "Unit": "USD"},
        "CostFilters": {},
        "CostTypes": {"IncludeCredit": False, "IncludeRefund": False},
        "TimePeriod": {
            "Start": datetime(2026, 10, 1, tzinfo=UTC),
            "End": datetime(2026, 11, 1, tzinfo=UTC),
        },
        "CalculatedSpend": {
            "ActualSpend": {"Amount": actual, "Unit": "USD"},
            "ForecastedSpend": {"Amount": forecast, "Unit": "USD"},
        },
    }


def test_budget_is_applicable_gross_forecast_headroom_not_budget_count() -> None:
    observed = int(datetime(2026, 10, 2, tzinfo=UTC).timestamp())
    result = budget_headroom([budget()], observed_at_epoch=observed, worst_case_microusd=20_000_000)
    assert result["available_headroom_microusd"] == 20_000_000
    assert "private-account-budget" not in json.dumps(result)
    with pytest.raises(LiveEvidenceError, match="headroom"):
        budget_headroom([budget()], observed_at_epoch=observed, worst_case_microusd=20_000_001)
    with pytest.raises(LiveEvidenceError, match="applicable"):
        budget_headroom([], observed_at_epoch=observed, worst_case_microusd=1)
    credited = budget()
    credited["CostTypes"]["IncludeCredit"] = True
    with pytest.raises(LiveEvidenceError, match="applicable"):
        budget_headroom([credited], observed_at_epoch=observed, worst_case_microusd=1)
    with pytest.raises(LiveEvidenceError, match="headroom"):
        budget_headroom(
            [budget(), budget(limit="90")],
            observed_at_epoch=observed,
            worst_case_microusd=11_000_000,
        )


def test_budget_fixture_and_collector_use_real_pinned_service_shapes() -> None:
    model = get_session().get_service_model("budgets")
    assert set(budget()["CostTypes"]) <= set(model.shape_for("CostTypes").members)
    assert "ShowFilterExpression" in model.operation_model("DescribeBudgets").input_shape.members
    filtered = budget()
    filtered["FilterExpression"] = {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon EC2"]}}
    with pytest.raises(LiveEvidenceError, match="applicable"):
        budget_headroom([filtered], observed_at_epoch=1_791_000_000, worst_case_microusd=1)


def test_workload_assumptions_cannot_be_claimed_as_verified_bounds() -> None:
    profile = json.loads(Path("docs/stage6/cost-workload-profile.json").read_text())
    assert profile["bound_enforcement_verified"] is False
    with pytest.raises(LiveEvidenceError, match="independently enforced"):
        qualifier._cost_envelope(100, profile, [])
