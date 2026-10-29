"""Policy decisions kept separate from Spark and SQL validation engines."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from .models import (
    EngineValidationResult,
    ExecutionMode,
    ExpectationFailure,
    ValidationRequest,
    ValidationStatus,
    ValidationSummary,
    utc_timestamp,
)


@dataclass(frozen=True)
class ValidationPolicy:
    blocking_severities: frozenset[str] = frozenset({"critical"})
    default_severity: str = "critical"

    def normalize_severity(self, value: object | None) -> str:
        raw = getattr(value, "value", value)
        return str(raw or self.default_severity).strip().lower()

    def apply(
        self,
        request: ValidationRequest,
        result: EngineValidationResult | None,
    ) -> ValidationSummary:
        executed = result is not None
        technical = result or EngineValidationResult(
            success=True,
            evaluated_expectations=0,
            successful_expectations=0,
        )
        critical: list[ExpectationFailure] = []
        warnings: list[ExpectationFailure] = []
        for failure in technical.failures:
            normalized = ExpectationFailure(
                expectation_type=failure.expectation_type,
                severity=self.normalize_severity(failure.severity),
                column=failure.column,
                observed_value=failure.observed_value,
                message=failure.message,
            )
            if normalized.severity in self.blocking_severities:
                critical.append(normalized)
            else:
                warnings.append(normalized)

        if request.execution_mode is ExecutionMode.OFF:
            status = ValidationStatus.PASSED
            merge_allowed = True
        elif request.execution_mode is ExecutionMode.REPORT:
            status = (
                ValidationStatus.WARNING
                if critical or warnings or not technical.success
                else ValidationStatus.PASSED
            )
            merge_allowed = True
        elif critical:
            status = ValidationStatus.FAILED
            merge_allowed = False
        elif warnings:
            status = ValidationStatus.WARNING
            merge_allowed = True
        elif not technical.success:
            status = ValidationStatus.FAILED
            merge_allowed = False
        else:
            status = ValidationStatus.PASSED
            merge_allowed = True

        identity_parts = [
            request.dataset_id,
            request.data_layer,
            request.run_id,
            request.contract_version,
            request.nessie_branch or "",
            request.snapshot_id or "",
            request.nessie_commit_hash or "",
            ",".join(f"{suite.name}@{suite.version}" for suite in request.suites),
        ]
        # Preserve phase-2 validation IDs for callers that have no source-kind
        # metadata; source-aware runs intentionally receive distinct identities.
        if request.source_kind:
            identity_parts.append(f"source={request.source_kind.value}")
        if request.suite_revision:
            identity_parts.append(f"suite_revision={request.suite_revision}")
        identity = "|".join(identity_parts)
        return ValidationSummary(
            validation_id=str(uuid5(NAMESPACE_URL, identity)),
            status=status,
            success=status is not ValidationStatus.FAILED,
            merge_allowed=merge_allowed,
            executed=executed,
            data_asset_ref=request.data_asset_ref,
            dataset_id=request.dataset_id,
            data_layer=request.data_layer,
            execution_engine=request.execution_engine,
            execution_mode=request.execution_mode,
            contract_version=request.contract_version,
            suite_names=tuple(suite.name for suite in request.suites),
            suite_versions={suite.name: suite.version for suite in request.suites},
            run_id=request.run_id,
            validation_timestamp=utc_timestamp(),
            nessie_branch=request.nessie_branch,
            snapshot_id=request.snapshot_id,
            nessie_commit_hash=request.nessie_commit_hash,
            evaluated_expectations=technical.evaluated_expectations,
            successful_expectations=technical.successful_expectations,
            failed_expectations=(
                technical.evaluated_expectations
                - technical.successful_expectations
            ),
            critical_failures=tuple(critical),
            warning_failures=tuple(warnings),
            result_ids=technical.result_ids,
            data_docs_url=technical.data_docs_url,
            metrics=technical.metrics,
            source_kind=request.source_kind,
            suite_revision=request.suite_revision,
            coverage=request.coverage,
            contract_status=request.contract_status,
        )
