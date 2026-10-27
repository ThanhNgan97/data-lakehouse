"""Validation engine adapters that produce technical, policy-free results."""

from __future__ import annotations

import json
import re
from typing import Any, Protocol

from .errors import ValidationExecutionError
from .models import (
    EngineValidationResult,
    ExpectationFailure,
    ValidationRequest,
)
from .suite_registry import SuiteRegistry


class DataFrameValidationEngine(Protocol):
    def validate_dataframe(
        self, dataframe: Any, request: ValidationRequest
    ) -> EngineValidationResult: ...


class TableValidationEngine(Protocol):
    def validate_table(self, request: ValidationRequest) -> EngineValidationResult: ...


def _safe_name(value: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    if not name:
        raise ValueError("GX resource name must contain an alphanumeric character")
    return name[:120]


class GreatExpectationsSparkEngine:
    """Stateless GX execution against an in-memory Spark DataFrame."""

    def __init__(self, suite_registry: SuiteRegistry) -> None:
        self.suite_registry = suite_registry

    def validate_dataframe(
        self, dataframe: Any, request: ValidationRequest
    ) -> EngineValidationResult:
        import great_expectations as gx
        from great_expectations.core import RunIdentifier

        context = gx.get_context(mode="ephemeral")
        base = _safe_name(f"{request.data_layer}_{request.dataset_id}_{request.run_id}")
        source = context.data_sources.add_spark(name=f"spark_{base}")
        asset = source.add_dataframe_asset(name=f"asset_{base}")
        batch = asset.add_batch_definition_whole_dataframe(name=f"batch_{base}")
        definitions = []
        for suite_ref in request.suites:
            metadata = self.suite_registry.resolve(
                suite_ref, request.execution_engine
            )
            try:
                payload = json.loads(
                    metadata.definition_path.read_text(encoding="utf-8")
                )
                suite = gx.ExpectationSuite(**payload)
                context.suites.add(suite)
                definitions.append(
                    context.validation_definitions.add(
                        gx.ValidationDefinition(
                            name=_safe_name(
                                f"{base}_{suite_ref.name}_{suite_ref.version}"
                            ),
                            data=batch,
                            suite=suite,
                        )
                    )
                )
            except Exception as exc:
                raise ValidationExecutionError(
                    f"Unable to prepare suite {suite_ref.name}@{suite_ref.version}"
                ) from exc
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(name=f"checkpoint_{base}", validation_definitions=definitions)
        )
        try:
            checkpoint_result = checkpoint.run(
                batch_parameters={"dataframe": dataframe},
                run_id=RunIdentifier(run_name=request.run_id),
            )
        except Exception as exc:
            raise ValidationExecutionError(
                f"GX Spark validation failed for {request.dataset_id}"
            ) from exc

        evaluated = successful = 0
        failures: list[ExpectationFailure] = []
        result_ids: list[str] = []
        for result_id, validation_result in checkpoint_result.run_results.items():
            result_ids.append(str(result_id))
            for result in validation_result.results:
                evaluated += 1
                if result.success:
                    successful += 1
                    continue
                config = result.expectation_config
                kwargs = getattr(config, "kwargs", {}) or {}
                severity = getattr(getattr(config, "severity", None), "value", None)
                failures.append(
                    ExpectationFailure(
                        expectation_type=str(
                            getattr(config, "type", None)
                            or getattr(config, "expectation_type", None)
                            or config.__class__.__name__
                        ),
                        severity=str(severity or "critical"),
                        column=kwargs.get("column"),
                        observed_value=(getattr(result, "result", {}) or {}).get(
                            "observed_value"
                        ),
                    )
                )
        return EngineValidationResult(
            success=bool(checkpoint_result.success),
            evaluated_expectations=evaluated,
            successful_expectations=successful,
            failures=tuple(failures),
            result_ids=tuple(result_ids),
        )


class SqlAlchemyTableEngine:
    """Stable SQL adapter boundary; rule execution is supplied by phase 4 suites."""

    def __init__(self, executor) -> None:
        self.executor = executor

    def validate_table(self, request: ValidationRequest) -> EngineValidationResult:
        try:
            return self.executor(request)
        except ValidationExecutionError:
            raise
        except Exception as exc:
            raise ValidationExecutionError(
                f"SQL validation failed for {request.data_asset_ref}"
            ) from exc
