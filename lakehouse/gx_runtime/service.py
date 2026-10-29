"""Stateless public validation functions used by service/CLI adapters."""

from __future__ import annotations

from typing import Any

from .engines import DataFrameValidationEngine, TableValidationEngine
from .errors import ValidationConfigurationError
from .models import (
    ExecutionEngine,
    ExecutionMode,
    SourceKind,
    SuiteReference,
    ContractStatus,
    ValidationCoverage,
    ValidationRequest,
)
from .policy import ValidationPolicy
from .storage import ResultStore


def _run(request, execute, result_store, policy):
    technical_result = None
    if request.execution_mode is not ExecutionMode.OFF:
        technical_result = execute()
    summary = (policy or ValidationPolicy()).apply(request, technical_result)
    result_store.save(summary)
    return summary


def _suite_references(
    suite_names: tuple[str, ...], suite_versions: dict[str, str]
) -> tuple[SuiteReference, ...]:
    missing = [name for name in suite_names if name not in suite_versions]
    extra = [name for name in suite_versions if name not in suite_names]
    if missing or extra:
        raise ValidationConfigurationError(
            "suite_names and suite_versions must describe the same suites; "
            f"missing_versions={missing}, extra_versions={extra}"
        )
    return tuple(SuiteReference(name, suite_versions[name]) for name in suite_names)


def validate_dataframe(
    dataframe: Any,
    *,
    data_asset_ref: str,
    dataset_id: str,
    data_layer: str,
    contract_version: str,
    suite_names: tuple[str, ...],
    suite_versions: dict[str, str],
    run_id: str,
    validator: DataFrameValidationEngine,
    result_store: ResultStore,
    execution_mode: ExecutionMode = ExecutionMode.ENFORCE,
    nessie_branch: str | None = None,
    snapshot_id: str | None = None,
    nessie_commit_hash: str | None = None,
    policy: ValidationPolicy | None = None,
    source_kind: SourceKind | None = None,
    suite_revision: str | None = None,
    coverage: ValidationCoverage = ValidationCoverage.CONTRACT,
    contract_status: ContractStatus = ContractStatus.APPROVED,
):
    request = ValidationRequest(
        data_asset_ref=data_asset_ref,
        dataset_id=dataset_id,
        data_layer=data_layer,
        execution_engine=ExecutionEngine.SPARK,
        contract_version=contract_version,
        suites=_suite_references(suite_names, suite_versions),
        run_id=run_id,
        execution_mode=execution_mode,
        nessie_branch=nessie_branch,
        snapshot_id=snapshot_id,
        nessie_commit_hash=nessie_commit_hash,
        source_kind=source_kind,
        suite_revision=suite_revision,
        coverage=coverage,
        contract_status=contract_status,
    )
    return _run(
        request,
        lambda: validator.validate_dataframe(dataframe, request),
        result_store,
        policy,
    )


def validate_table(
    *,
    data_asset_ref: str,
    dataset_id: str,
    data_layer: str,
    contract_version: str,
    suite_names: tuple[str, ...],
    suite_versions: dict[str, str],
    run_id: str,
    nessie_branch: str,
    snapshot_id: str,
    nessie_commit_hash: str,
    validator: TableValidationEngine,
    result_store: ResultStore,
    execution_mode: ExecutionMode = ExecutionMode.ENFORCE,
    policy: ValidationPolicy | None = None,
    source_kind: SourceKind | None = None,
    suite_revision: str | None = None,
    coverage: ValidationCoverage = ValidationCoverage.CONTRACT,
    contract_status: ContractStatus = ContractStatus.APPROVED,
):
    request = ValidationRequest(
        data_asset_ref=data_asset_ref,
        dataset_id=dataset_id,
        data_layer=data_layer,
        execution_engine=ExecutionEngine.SQL,
        contract_version=contract_version,
        suites=_suite_references(suite_names, suite_versions),
        run_id=run_id,
        execution_mode=execution_mode,
        nessie_branch=nessie_branch,
        snapshot_id=snapshot_id,
        nessie_commit_hash=nessie_commit_hash,
        source_kind=source_kind,
        suite_revision=suite_revision,
        coverage=coverage,
        contract_status=contract_status,
    )
    if request.execution_engine is not ExecutionEngine.SQL:
        raise ValidationConfigurationError("Table validation requires SQL engine")
    return _run(
        request,
        lambda: validator.validate_table(request),
        result_store,
        policy,
    )
