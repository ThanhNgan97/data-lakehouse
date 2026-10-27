"""Engine-neutral request and result contracts for lakehouse validation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .errors import ValidationConfigurationError


class ExecutionEngine(str, Enum):
    SPARK = "spark"
    SQL = "sql"


class ExecutionMode(str, Enum):
    OFF = "off"
    REPORT = "report"
    ENFORCE = "enforce"


class ValidationStatus(str, Enum):
    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"


@dataclass(frozen=True)
class SuiteReference:
    name: str
    version: str

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.version.strip():
            raise ValidationConfigurationError(
                "Suite name and version must both be non-empty"
            )


@dataclass(frozen=True)
class ValidationRequest:
    data_asset_ref: str
    dataset_id: str
    data_layer: str
    execution_engine: ExecutionEngine
    contract_version: str
    suites: tuple[SuiteReference, ...]
    run_id: str
    execution_mode: ExecutionMode = ExecutionMode.ENFORCE
    nessie_branch: str | None = None
    snapshot_id: str | None = None
    nessie_commit_hash: str | None = None

    def __post_init__(self) -> None:
        required = {
            "data_asset_ref": self.data_asset_ref,
            "dataset_id": self.dataset_id,
            "data_layer": self.data_layer,
            "contract_version": self.contract_version,
            "run_id": self.run_id,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValidationConfigurationError(
                f"Validation request fields must be non-empty: {', '.join(missing)}"
            )
        if not self.suites:
            raise ValidationConfigurationError("At least one suite is required")
        if self.execution_engine is ExecutionEngine.SQL:
            identity = {
                "nessie_branch": self.nessie_branch,
                "snapshot_id": self.snapshot_id,
                "nessie_commit_hash": self.nessie_commit_hash,
            }
            absent = [name for name, value in identity.items() if not value]
            if absent:
                raise ValidationConfigurationError(
                    "Table validation requires snapshot identity: "
                    + ", ".join(absent)
                )


@dataclass(frozen=True)
class ExpectationFailure:
    expectation_type: str
    severity: str = "critical"
    column: str | None = None
    observed_value: Any = None
    message: str | None = None


@dataclass(frozen=True)
class EngineValidationResult:
    """Technical output produced by a validation engine before policy."""

    success: bool
    evaluated_expectations: int
    successful_expectations: int
    failures: tuple[ExpectationFailure, ...] = ()
    result_ids: tuple[str, ...] = ()
    data_docs_url: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationSummary:
    """Serializable policy decision persisted outside the validator process."""

    validation_id: str
    status: ValidationStatus
    success: bool
    merge_allowed: bool
    executed: bool
    data_asset_ref: str
    dataset_id: str
    data_layer: str
    execution_engine: ExecutionEngine
    execution_mode: ExecutionMode
    contract_version: str
    suite_names: tuple[str, ...]
    suite_versions: dict[str, str]
    run_id: str
    validation_timestamp: str
    nessie_branch: str | None
    snapshot_id: str | None
    nessie_commit_hash: str | None
    evaluated_expectations: int
    successful_expectations: int
    failed_expectations: int
    critical_failures: tuple[ExpectationFailure, ...]
    warning_failures: tuple[ExpectationFailure, ...]
    result_ids: tuple[str, ...]
    data_docs_url: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        payload["execution_engine"] = self.execution_engine.value
        payload["execution_mode"] = self.execution_mode.value
        payload["suite_names"] = list(self.suite_names)
        payload["result_ids"] = list(self.result_ids)
        payload["critical_failures"] = [
            asdict(failure) for failure in self.critical_failures
        ]
        payload["warning_failures"] = [
            asdict(failure) for failure in self.warning_failures
        ]
        return payload


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()
