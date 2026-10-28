"""Dataset/layer/contract routing independent of ingestion technology."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import ContractRegistry
from .errors import ValidationConfigurationError
from .models import ExecutionEngine, SourceKind, SuiteReference, ValidationRequest
from .suite_registry import SuiteRegistry


@dataclass(frozen=True)
class ValidationPlan:
    dataset_id: str
    data_layer: str
    contract_version: str
    data_asset_ref: str
    execution_engine: ExecutionEngine
    suites: tuple[SuiteReference, ...]
    source_kind: SourceKind

    def request(
        self,
        *,
        run_id: str,
        nessie_branch: str | None = None,
        snapshot_id: str | None = None,
        nessie_commit_hash: str | None = None,
    ) -> ValidationRequest:
        return ValidationRequest(
            data_asset_ref=self.data_asset_ref,
            dataset_id=self.dataset_id,
            data_layer=self.data_layer,
            execution_engine=self.execution_engine,
            contract_version=self.contract_version,
            suites=self.suites,
            run_id=run_id,
            nessie_branch=nessie_branch,
            snapshot_id=snapshot_id,
            nessie_commit_hash=nessie_commit_hash,
            source_kind=self.source_kind,
        )


class DatasetValidationRegistry:
    """Resolve by dataset identity, never by file/API/database implementation."""

    def __init__(
        self,
        registry_path: str | Path,
        *,
        contract_registry: ContractRegistry,
        suite_registry: SuiteRegistry,
    ) -> None:
        self.registry_path = Path(registry_path)
        self.contract_registry = contract_registry
        self.suite_registry = suite_registry
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            self._routes = {
                (item["dataset_id"], item["layer"], item["contract_version"]): item
                for item in payload["routes"]
            }
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValidationConfigurationError(
                f"Unable to load validation registry {self.registry_path}"
            ) from exc
        if len(self._routes) != len(payload["routes"]):
            raise ValidationConfigurationError(
                "Validation routes must be unique by dataset, layer, and contract"
            )

    def resolve(
        self,
        *,
        dataset_id: str,
        data_layer: str,
        contract_version: str | None,
        execution_engine: ExecutionEngine,
        source_kind: SourceKind,
    ) -> ValidationPlan:
        candidates = [
            route
            for (route_dataset, route_layer, route_contract), route in self._routes.items()
            if route_dataset == dataset_id
            and route_layer == data_layer
            and (contract_version is None or route_contract == contract_version)
        ]
        if contract_version is None:
            effective = self.contract_registry.effective_versions(dataset_id)
            candidates = [
                route
                for route in candidates
                if str(route["contract_version"]) in effective
            ]
        if not candidates:
            raise ValidationConfigurationError(
                f"No validation route for {dataset_id}/{data_layer}@{contract_version}"
            )
        if contract_version is None:
            route = max(
                candidates,
                key=lambda item: _version_key(str(item["contract_version"])),
            )
            contract_version = str(route["contract_version"])
        else:
            route = candidates[0]
        supported_sources = frozenset(
            SourceKind(value) for value in route["supported_source_kinds"]
        )
        if source_kind not in supported_sources:
            if source_kind is SourceKind.STREAMING:
                raise ValidationConfigurationError(
                    f"Streaming validation is not enabled for {dataset_id}/{data_layer}"
                )
            raise ValidationConfigurationError(
                f"Source kind {source_kind.value} is not supported for "
                f"{dataset_id}/{data_layer}"
            )
        try:
            raw_suites = route["suites"][execution_engine.value]
        except KeyError as exc:
            raise ValidationConfigurationError(
                f"Engine {execution_engine.value} is not supported for "
                f"{dataset_id}/{data_layer}"
            ) from exc

        self.contract_registry.resolve(dataset_id, contract_version)
        suites = tuple(
            SuiteReference(item["name"], item["version"])
            for item in raw_suites
        )
        for suite in suites:
            metadata = self.suite_registry.resolve(suite, execution_engine)
            if metadata.contract_version != contract_version:
                raise ValidationConfigurationError(
                    f"Suite {suite.name}@{suite.version} does not implement "
                    f"contract {contract_version}"
                )
        return ValidationPlan(
            dataset_id=dataset_id,
            data_layer=data_layer,
            contract_version=contract_version,
            data_asset_ref=route["data_asset_ref"],
            execution_engine=execution_engine,
            suites=suites,
            source_kind=source_kind,
        )


def _version_key(value: str) -> tuple[tuple[int, object], ...]:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.lower())
        for part in value.replace("-", ".").split(".")
    )
