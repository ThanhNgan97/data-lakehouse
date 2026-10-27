"""Git-backed, versioned suite routing metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from .errors import SuiteNotFoundError, ValidationConfigurationError
from .models import ExecutionEngine, SuiteReference


@dataclass(frozen=True)
class SuiteMetadata:
    name: str
    version: str
    supported_engines: frozenset[ExecutionEngine]
    contract_version: str
    effective_from: date
    definition_path: Path
    definition_paths: dict[ExecutionEngine, Path]


class SuiteRegistry:
    def __init__(self, registry_path: str | Path) -> None:
        self.registry_path = Path(registry_path)
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValidationConfigurationError(
                f"Unable to load suite registry {self.registry_path}"
            ) from exc
        self._suites: dict[tuple[str, str], SuiteMetadata] = {}
        for item in payload.get("suites", []):
            definitions = {
                ExecutionEngine(engine): (
                    self.registry_path.parent / path
                ).resolve()
                for engine, path in item.get("definitions", {}).items()
            }
            legacy_definition = item.get("definition")
            metadata = SuiteMetadata(
                name=item["name"],
                version=item["version"],
                supported_engines=frozenset(
                    ExecutionEngine(engine) for engine in item["supported_engines"]
                ),
                contract_version=item["contract_version"],
                effective_from=date.fromisoformat(item["effective_from"]),
                definition_path=(
                    (self.registry_path.parent / legacy_definition).resolve()
                    if legacy_definition
                    else Path()
                ),
                definition_paths=definitions,
            )
            self._suites[(metadata.name, metadata.version)] = metadata

    def resolve(
        self, suite: SuiteReference, engine: ExecutionEngine
    ) -> SuiteMetadata:
        try:
            metadata = self._suites[(suite.name, suite.version)]
        except KeyError as exc:
            raise SuiteNotFoundError(
                f"Suite {suite.name!r} version {suite.version!r} is not registered"
            ) from exc
        if engine not in metadata.supported_engines:
            raise ValidationConfigurationError(
                f"Suite {suite.name}@{suite.version} does not support {engine.value}"
            )
        definition_path = metadata.definition_paths.get(
            engine, metadata.definition_path
        )
        if not definition_path.is_file():
            raise SuiteNotFoundError(
                f"Suite definition not found: {definition_path}"
            )
        return replace(metadata, definition_path=definition_path)
