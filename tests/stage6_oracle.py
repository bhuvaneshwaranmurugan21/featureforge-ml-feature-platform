"""Independent primitive-record oracle for Stage 6 expected authorities.

This module intentionally imports no production FeatureForge code.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def manifest_digest(value: dict[str, Any]) -> str:
    body = dict(value)
    body.pop("manifest_digest", None)
    return sha(body)


def cost(subtotal_microusd: int, safety_margin_bps: int) -> int:
    return (subtotal_microusd * (10_000 + safety_margin_bps) + 9_999) // 10_000


def cost_projection(raw: dict[str, Any]) -> dict[str, Any]:
    """Calculate the entire cost authority from primitive quantities and rates."""
    lines = []
    for line in raw["lines"]:
        quantity = line["quantity_millionths"]
        rate = line["unit_cost_microusd"]
        if type(quantity) is not int or type(rate) is not int or min(quantity, rate) < 0:
            raise ValueError("invalid primitive cost quantity or rate")
        lines.append(line | {"extended_microusd": -(-(quantity * rate) // 1_000_000)})
    subtotal = sum(line["extended_microusd"] for line in lines)
    worst = cost(subtotal, raw["safety_margin_bps"])
    return {
        "contract": "stage6-cost-envelope-v1",
        "currency": "USD",
        "lines": lines,
        "subtotal_microusd": subtotal,
        "worst_case_microusd": worst,
        "admitted": worst <= raw["maximum_microusd"],
        "maximum_microusd": raw["maximum_microusd"],
        "pricing_observed_at_epoch": raw["pricing_observed_at_epoch"],
        "safety_margin_bps": raw["safety_margin_bps"],
    }


def admission_projection(raw: dict[str, Any]) -> dict[str, Any]:
    """Independent admission model: no production objects or serialized decisions."""
    manifest = raw["manifest"]
    lease = raw["lease"]
    inventory = raw["inventory"]
    envelope = cost_projection(raw["cost"])
    now = raw["observed_at_epoch"]
    available = raw["available_quotas"]
    required = raw["required_quotas"]
    predicates = {
        "account": raw["observed_account_fingerprint"] == manifest["account_fingerprint"],
        "region": raw["observed_region"] == manifest["region"],
        "lease": lease["owner"] == manifest["run_id"]
        and lease["source_commit"] == manifest["source_commit"]
        and lease["heartbeat_at_epoch"] <= now < lease["expires_at_epoch"],
        "inventory": inventory["namespace"] == manifest["namespace"]
        and not inventory["items"]
        and 0 <= now - inventory["observed_at_epoch"] <= 3_600,
        "pricing": 0 <= now - envelope["pricing_observed_at_epoch"] <= 3_600,
        "quota": set(available) == set(required)
        and all(amount >= 0 for amount in (*available.values(), *required.values()))
        and all(available[name] >= amount for name, amount in required.items()),
        "cost": envelope["maximum_microusd"] == manifest["max_cost_microusd"]
        and envelope["safety_margin_bps"] == manifest["safety_margin_bps"]
        and envelope["admitted"],
    }
    failed = [name for name, passes in predicates.items() if not passes]
    if failed:
        return {"decision": "DENIED", "failed_checks": failed}
    decision = {
        "manifest_digest": manifest_digest(manifest),
        "account_fingerprint": raw["observed_account_fingerprint"],
        "region": raw["observed_region"],
        "lease_digest": sha(lease),
        "inventory_digest": sha(inventory),
        "quota_digest": sha(available),
        "cost_digest": sha(envelope),
        "decision": "ELIGIBLE",
    }
    return decision | {"admission_digest": sha(decision)}
