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
