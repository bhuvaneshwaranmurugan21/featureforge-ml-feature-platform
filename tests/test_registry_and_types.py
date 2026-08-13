from dataclasses import replace

import pytest

from featureforge.computation import FeatureTypeError, compute
from featureforge.definitions import payment_features
from featureforge.model import FeatureDefinition, PaymentEvent
from featureforge.registry import DefinitionConflict, FeatureRegistry


def test_registry_is_immutable_per_version() -> None:
    definition = payment_features()[0]
    registry = FeatureRegistry()
    assert registry.register(definition) == definition.definition_digest
    assert registry.register(definition) == definition.definition_digest
    assert registry.get(definition.definition_id) == definition
    assert registry.definitions() == (definition,)
    with pytest.raises(DefinitionConflict):
        registry.register(replace(definition, description="changed"))


def test_value_type_contract_is_enforced() -> None:
    bad = FeatureDefinition("ratio", 1, "integer", 100, "failed_payment_ratio", 100, "bad")
    events = (PaymentEvent("e", "c", 10, 10, 100, "failed", 0.5),)
    with pytest.raises(FeatureTypeError):
        compute(bad, events, 20)


def test_domain_validation() -> None:
    with pytest.raises(ValueError, match="amount"):
        PaymentEvent("e", "c", 1, 1, -1, "failed", 0.5)
    with pytest.raises(ValueError, match="risk"):
        PaymentEvent("e", "c", 1, 1, 1, "failed", 2.0)
    with pytest.raises(ValueError, match="TTL"):
        FeatureDefinition("x", 1, "integer", 0, "transaction_count", None, "x")
