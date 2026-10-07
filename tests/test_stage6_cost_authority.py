from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
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


def test_qualification_client_has_one_physical_attempt_and_explicit_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = boto3.Session(aws_access_key_id="local-shape-only", aws_secret_access_key="local")
    monkeypatch.setattr(qualifier.boto3, "client", session.client)
    monkeypatch.setenv("AWS_MAX_ATTEMPTS", "8")
    client = qualifier._client("ce", "us-east-1")
    try:
        assert client.meta.config.retries["total_max_attempts"] == 1
        assert client.meta.config.connect_timeout == 5
        assert client.meta.config.read_timeout == 10
        assert client.meta.endpoint_url == "https://ce.us-east-1.amazonaws.com"
    finally:
        client.close()


def test_wrong_account_rejects_before_any_inventory_or_price_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    class Identity:
        def get_caller_identity(self) -> dict[str, str]:
            return {"Account": "000000000000", "Arn": "wrong"}

    def local_client(service: str, region: str = qualifier.REGION) -> Any:
        calls.append(service)
        assert service == "sts"
        return Identity()

    monkeypatch.setattr(qualifier, "_client", local_client)
    with pytest.raises(LiveEvidenceError, match="authorized account"):
        qualifier.qualify("s6-plan-20260930", "part3-stage6-aws-plan-qualification")
    assert calls == ["sts"]


def test_cost_visibility_rejects_incomplete_page_without_another_paid_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = int(datetime(2026, 10, 2, tzinfo=UTC).timestamp())
    requests = []

    class Billing:
        def describe_budgets(self, **kwargs: Any) -> dict[str, Any]:
            return {"Budgets": [budget()]}

        def get_cost_and_usage(self, **kwargs: Any) -> dict[str, Any]:
            requests.append(kwargs)
            return {"ResultsByTime": [{}], "NextPageToken": "unread"}

    monkeypatch.setattr(qualifier, "_client", lambda *args: Billing())
    with pytest.raises(LiveEvidenceError, match="one-request"):
        qualifier._financial_visibility("local-account", observed, 1)
    assert len(requests) == 1
    assert requests[0]["TimePeriod"] == {"Start": "2026-10-01", "End": "2026-10-03"}


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


def test_kms_price_selector_excludes_asymmetric_and_data_key_pair_rates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def kms_product(usage: str, rate: str, description: str) -> dict[str, Any]:
        value = product(rate)
        value["product"]["attributes"]["usagetype"] = usage
        dimension = value["terms"]["OnDemand"]["term"]["priceDimensions"]["dimension"]
        dimension["description"] = description
        return value

    standard = kms_product(
        "ap-southeast-2-KMS-Requests",
        "0.000003",
        "$0.03 per 10000 KMS requests in Asia Pacific (Sydney)",
    )
    rsa_pair = kms_product(
        "ap-southeast-2-KMS-Requests-GenerateDatakeyPair-RSA",
        "0.0012",
        "$12 per 10000 RSA Generate Data Key Pair Requests in Asia Pacific (Sydney)",
    )
    asymmetric = kms_product(
        "ap-southeast-2-KMS-Requests-Asymmetric",
        "0.000015",
        "$0.15 per 10000 KMS Asymmetric Requests in Asia Pacific (Sydney)",
    )

    class Catalog:
        def get_products(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["ServiceCode"] == "awskms"
            return {
                "PriceList": [
                    json.dumps(standard),
                    json.dumps(rsa_pair),
                    json.dumps(asymmetric),
                ]
            }

    monkeypatch.setattr(qualifier, "PRICE_SERVICES", ("awskms",))
    monkeypatch.setattr(qualifier, "_client", lambda *args: Catalog())
    profile = {
        "lines": [
            {
                "component": "kms-requests",
                **qualifier.EXPECTED_KMS_REQUEST_SELECTOR,
            }
        ]
    }

    result = qualifier._pricing(1_800_000_000, profile)
    assert result[0]["matching_dimension_count"] == 1
    assert result[0]["unit_cost_microusd"] == 3
    assert result[0]["selected_dimension"]["description"].startswith("$0.03")


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


def modern_budget() -> dict[str, Any]:
    value = budget()
    value.pop("CostTypes")
    value["FilterExpression"] = {
        "Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}
    }
    value["Metrics"] = ["UnblendedCost"]
    return value


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


def test_modern_account_wide_gross_budget_is_equivalent_to_legacy_shape() -> None:
    observed = int(datetime(2026, 10, 2, tzinfo=UTC).timestamp())
    current = modern_budget()
    result = budget_headroom(
        [current], observed_at_epoch=observed, worst_case_microusd=20_000_000
    )
    assert result["available_headroom_microusd"] == 20_000_000
    current["FilterExpression"]["Not"]["Dimensions"] = {
        "Key": "RECORD_TYPE",
        "Values": ["Refund", "Credit"],
        "MatchOptions": ["EQUALS"],
    }
    assert budget_headroom(
        [current], observed_at_epoch=observed, worst_case_microusd=20_000_000
    )["headroom_verified"] is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("FilterExpression", {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon EC2"]}}),
        (
            "FilterExpression",
            {
                "Not": {
                    "Dimensions": {
                        "Key": "RECORD_TYPE",
                        "Values": ["Credit", "Refund", "Discount"],
                    }
                }
            },
        ),
        (
            "FilterExpression",
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": [["Credit"], "Refund"]}}},
        ),
        ("Metrics", ["NetUnblendedCost"]),
        ("BillingViewArn", "arn:aws:billing::123456789012:billingview/scoped"),
        ("CostTypes", {"IncludeCredit": False, "IncludeRefund": False}),
    ],
)
def test_modern_budget_rejects_scope_or_cost_model_drift(field: str, value: Any) -> None:
    observed = int(datetime(2026, 10, 2, tzinfo=UTC).timestamp())
    current = modern_budget()
    current[field] = value
    with pytest.raises(LiveEvidenceError, match="applicable"):
        budget_headroom([current], observed_at_epoch=observed, worst_case_microusd=1)


def test_budget_fixture_and_collector_use_real_pinned_service_shapes() -> None:
    model = get_session().get_service_model("budgets")
    assert set(budget()["CostTypes"]) <= set(model.shape_for("CostTypes").members)
    assert "ShowFilterExpression" in model.operation_model("DescribeBudgets").input_shape.members
    assert modern_budget()["Metrics"] == ["UnblendedCost"]
    filtered = budget()
    filtered["FilterExpression"] = {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon EC2"]}}
    with pytest.raises(LiveEvidenceError, match="applicable"):
        budget_headroom([filtered], observed_at_epoch=1_791_000_000, worst_case_microusd=1)


def test_plan_bound_scope_is_verified_without_overstating_execution_or_teardown() -> None:
    profile = json.loads(Path("docs/stage6/cost-workload-profile.json").read_text())
    assert profile["planning_bounds_verified"] is True
    assert profile["managed_runtime_executed"] is False
    assert profile["teardown_executed"] is False
    assert profile["billed_cost_observed"] is False
    rates = [
        {"component": line["component"], "unit_cost_microusd": 1}
        for line in profile["lines"]
    ]
    result = qualifier._cost_envelope(100, profile, rates)
    assert result["admitted"] is True

    for field, value in (
        ("planning_bounds_verified", False),
        ("verification_scope", "UNBOUNDED"),
        ("cost_horizon_days", 31),
        ("maximum_workflow_executions", 2),
        ("planned_max_input_rows", 1_001),
        ("planned_max_output_rows", 5_001),
        ("maximum_explicit_object_bytes", 1),
        ("maximum_explicit_run_objects", 10),
        ("managed_runtime_executed", True),
        ("teardown_executed", True),
        ("billed_cost_observed", True),
    ):
        mutated = dict(profile)
        mutated[field] = value
        with pytest.raises(LiveEvidenceError, match="authority"):
            qualifier._cost_envelope(100, mutated, rates)

    changed_quantity = json.loads(json.dumps(profile))
    changed_quantity["lines"][0]["quantity_millionths"] += 1
    with pytest.raises(LiveEvidenceError, match="quantities"):
        qualifier._cost_envelope(100, changed_quantity, rates)

    changed_kms_selector = json.loads(json.dumps(profile))
    next(
        line for line in changed_kms_selector["lines"] if line["component"] == "kms-requests"
    )["usage_pattern"] = "KMS-Requests"
    with pytest.raises(LiveEvidenceError, match="KMS price selector"):
        qualifier._cost_envelope(100, changed_kms_selector, rates)


def test_cost_explorer_published_request_price_is_explicit_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = json.loads(Path("docs/stage6/cost-workload-profile.json").read_text())
    monkeypatch.setattr(qualifier, "PRICE_SERVICES", ())
    observed = int(datetime(2026, 10, 5, tzinfo=UTC).timestamp())
    rates = qualifier._pricing(observed, profile)
    assert rates == [
        {
            "component": "cost-explorer-requests",
            "service_code": "AWSCostExplorer",
            "region": "ACCOUNT_GLOBAL",
            "unit_cost_microusd": 10_000,
            "conservative_maximum_tier": True,
            "matching_dimension_count": 1,
            "selected_dimension": {
                "description": "primary billing view API request",
                "effective_date": "2026-10-05",
                "pricing_scope": "PUBLISHED_FIXED_RATE",
                "rate_id": "aws-cost-explorer-primary-billing-view-request",
                "source": qualifier.COST_EXPLORER_PRICE_URL,
                "unit": "Request",
            },
        }
    ]
    changed = json.loads(json.dumps(profile))
    next(
        line for line in changed["lines"] if line["component"] == "cost-explorer-requests"
    )["fixed_unit_cost_microusd"] = 9_999
    with pytest.raises(LiveEvidenceError, match="price authority drift"):
        qualifier._pricing(observed, changed)
    stale = int(datetime(2026, 10, 13, tzinfo=UTC).timestamp())
    with pytest.raises(LiveEvidenceError, match="stale or future"):
        qualifier._pricing(stale, profile)
