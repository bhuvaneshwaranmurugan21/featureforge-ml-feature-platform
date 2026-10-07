"""Reject transport coercions at the versioned managed authority boundary."""

from dataclasses import replace
from typing import Any

import pytest

from featureforge.managed import (
    AdmissionDenied,
    CostLine,
    ManagedContractError,
    ManagedRunManifest,
    admit_managed_run,
)
from tests.test_stage6_managed import authority, costs, inventory, lease, manifest


@pytest.mark.parametrize(
    "field", ["max_input_rows", "max_output_rows", "max_cost_microusd", "safety_margin_bps"]
)
@pytest.mark.parametrize("value", [True, False, "100", 100.5, 100.0, None])
def test_manifest_rejects_non_integer_authority(field: str, value: Any) -> None:
    payload = manifest().as_dict()
    payload[field] = value
    with pytest.raises(ManagedContractError, match="integer without coercion"):
        ManagedRunManifest.from_dict(payload)
    with pytest.raises(ManagedContractError, match="integer without coercion"):
        replace(manifest(), **{field: value})


@pytest.mark.parametrize("value", [None, "digest", 100, {}])
def test_artifact_collection_must_be_a_list(value: Any) -> None:
    payload = manifest().as_dict() | {"artifact_digests": value}
    with pytest.raises(ManagedContractError, match="must be a list"):
        ManagedRunManifest.from_dict(payload)


@pytest.mark.parametrize("value", [None, "object", {}, {"bucket": "bucket"}])
def test_input_authority_is_closed(value: Any) -> None:
    payload = manifest().as_dict() | {"inputs": [value]}
    with pytest.raises(ManagedContractError, match="missing or unexpected fields"):
        ManagedRunManifest.from_dict(payload)


@pytest.mark.parametrize("value", [True, False, "100", 100.0, None])
@pytest.mark.parametrize(
    "factory,field",
    [
        (lease, "acquired_at_epoch"),
        (lease, "heartbeat_at_epoch"),
        (lease, "expires_at_epoch"),
        (inventory, "observed_at_epoch"),
        (costs, "pricing_observed_at_epoch"),
        (costs, "safety_margin_bps"),
        (costs, "maximum_microusd"),
        (authority, "state_serial"),
        (authority, "created_at_epoch"),
        (authority, "expires_at_epoch"),
    ],
)
def test_all_numeric_authorities_reject_coercion(factory: Any, field: str, value: Any) -> None:
    with pytest.raises(ManagedContractError, match="integer without coercion"):
        replace(factory(), **{field: value})


@pytest.mark.parametrize("value", [True, 1.0, "1", None])
def test_quota_authority_rejects_coercion(value: Any) -> None:
    checked = manifest()
    with pytest.raises(AdmissionDenied, match="integer authorities"):
        admit_managed_run(
            checked,
            observed_account_fingerprint=checked.account_fingerprint,
            observed_region=checked.region,
            lease=lease(),
            inventory=inventory(),
            available_quotas={"glue-runs": value},
            required_quotas={"glue-runs": 1},
            cost=costs(),
            observed_at_epoch=1200,
        )


@pytest.mark.parametrize(
    "field", ["quantity_millionths", "unit_cost_microusd", "extended_microusd"]
)
@pytest.mark.parametrize("value", [True, 0.0, "0", None])
def test_cost_line_requires_exact_integer_units(field: str, value: Any) -> None:
    with pytest.raises(ManagedContractError, match="integer without coercion"):
        replace(CostLine("requests", 0, 0, 0), **{field: value})
