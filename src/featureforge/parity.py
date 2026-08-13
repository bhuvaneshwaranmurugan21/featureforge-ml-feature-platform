"""Offline/online parity proof before publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from featureforge.canonical import digest
from featureforge.model import FeatureValue


@dataclass(frozen=True)
class ParityReport:
    matched: bool
    compared: int
    mismatches: tuple[dict[str, Any], ...]
    proof_digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "compared": self.compared,
            "matched": self.matched,
            "mismatches": self.mismatches,
            "proof_digest": self.proof_digest,
        }


def _key(value: FeatureValue) -> tuple[str, str]:
    return value.customer_id, value.feature_name


def latest(values: tuple[FeatureValue, ...]) -> dict[tuple[str, str], FeatureValue]:
    result: dict[tuple[str, str], FeatureValue] = {}
    for value in values:
        key = _key(value)
        previous = result.get(key)
        if previous is None or (value.event_time, value.knowledge_time) > (
            previous.event_time,
            previous.knowledge_time,
        ):
            result[key] = value
    return result


def compare_parity(
    offline: tuple[FeatureValue, ...], online: tuple[FeatureValue, ...]
) -> ParityReport:
    expected = latest(offline)
    actual = {_key(value): value for value in online}
    mismatches: list[dict[str, Any]] = []
    for key in sorted(set(expected) | set(actual)):
        left = expected.get(key)
        right = actual.get(key)
        if left is None or right is None or left.as_dict() != right.as_dict():
            mismatches.append(
                {
                    "customer_id": key[0],
                    "expected": None if left is None else left.as_dict(),
                    "feature_name": key[1],
                    "online": None if right is None else right.as_dict(),
                }
            )
    compared = len(set(expected) | set(actual))
    proof = {"compared": compared, "mismatches": mismatches}
    return ParityReport(not mismatches, compared, tuple(mismatches), digest(proof))
