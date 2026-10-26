"""Stable result types returned by the GX integration layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GXExpectationFailure:
    """One failed Expectation normalized independently of GX internals."""

    expectation_type: str
    severity: str
    column: str | None
    observed_value: Any


@dataclass(frozen=True)
class GXValidationSummary:
    """Serializable validation outcome used by Airflow-facing code."""

    success: bool
    raw_gx_success: bool
    dataset_id: str
    data_layer: str
    run_id: str
    suite_names: tuple[str, ...]
    evaluated_expectations: int
    successful_expectations: int
    failed_expectations: int
    critical_failures: tuple[GXExpectationFailure, ...]
    warning_failures: tuple[GXExpectationFailure, ...]
    validation_result_ids: tuple[str, ...]
    table_name: str | None = None
    nessie_branch: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return primitives suitable for logs, JSON, and Airflow XCom."""
        return {
            "success": self.success,
            "raw_gx_success": self.raw_gx_success,
            "dataset_id": self.dataset_id,
            "data_layer": self.data_layer,
            "run_id": self.run_id,
            "suite_names": list(self.suite_names),
            "evaluated_expectations": self.evaluated_expectations,
            "successful_expectations": self.successful_expectations,
            "failed_expectations": self.failed_expectations,
            "critical_failures": [
                failure.__dict__ for failure in self.critical_failures
            ],
            "warning_failures": [
                failure.__dict__ for failure in self.warning_failures
            ],
            "validation_result_ids": list(self.validation_result_ids),
            "table_name": self.table_name,
            "nessie_branch": self.nessie_branch,
        }
