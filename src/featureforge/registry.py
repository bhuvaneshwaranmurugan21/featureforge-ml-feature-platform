"""Immutable feature-definition registry."""

from __future__ import annotations

from featureforge.model import FeatureDefinition


class DefinitionConflict(RuntimeError):
    pass


class FeatureRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, FeatureDefinition] = {}

    def register(self, definition: FeatureDefinition) -> str:
        existing = self._definitions.get(definition.definition_id)
        if existing and existing.definition_digest != definition.definition_digest:
            raise DefinitionConflict(
                f"{definition.definition_id} already exists with another digest"
            )
        self._definitions[definition.definition_id] = definition
        return definition.definition_digest

    def get(self, definition_id: str) -> FeatureDefinition:
        return self._definitions[definition_id]

    def definitions(self) -> tuple[FeatureDefinition, ...]:
        return tuple(self._definitions[key] for key in sorted(self._definitions))
