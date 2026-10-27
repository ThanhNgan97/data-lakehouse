"""Validation engine adapters that produce technical, policy-free results."""

from __future__ import annotations

import json
import re
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

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
            if metadata.contract_version != request.contract_version:
                raise ValidationExecutionError(
                    f"Suite {suite_ref.name}@{suite_ref.version} contract mismatch"
                )
            try:
                payload = json.loads(
                    metadata.definition_path.read_text(encoding="utf-8")
                )
                if payload.get("name") != metadata.name:
                    raise ValidationExecutionError(
                        f"Suite identity mismatch in {metadata.definition_path}"
                    )
                if (payload.get("meta") or {}).get("contract_version") != request.contract_version:
                    raise ValidationExecutionError(
                        f"Suite {suite_ref.name}@{suite_ref.version} contract mismatch"
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


class VersionedSqlSuiteExecutor:
    """Execute Git-versioned, read-only SQL expectations through SQLAlchemy."""

    _ASSET_PATTERN = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*"
        r"\.[A-Za-z_][A-Za-z0-9_]*"
        r"\.[A-Za-z_][A-Za-z0-9_]*$"
    )

    def __init__(self, suite_registry: SuiteRegistry, sqlalchemy_engine: Any) -> None:
        self.suite_registry = suite_registry
        self.sqlalchemy_engine = sqlalchemy_engine

    @staticmethod
    def _matches(observed: Any, success: dict[str, Any]) -> bool:
        operator = success["operator"]
        expected = success["value"]
        if observed is None:
            return False
        if operator == "eq":
            return observed == expected
        if operator == "lte":
            return observed <= expected
        if operator == "gte":
            return observed >= expected
        if operator == "between":
            return expected[0] <= observed <= expected[1]
        raise ValidationExecutionError(f"Unsupported SQL expectation operator {operator}")

    def __call__(self, request: ValidationRequest) -> EngineValidationResult:
        from sqlalchemy import text

        if not self._ASSET_PATTERN.fullmatch(request.data_asset_ref):
            raise ValidationExecutionError(
                f"Unsafe SQL data_asset_ref {request.data_asset_ref!r}"
            )
        evaluated = successful = 0
        failures: list[ExpectationFailure] = []
        result_ids: list[str] = []
        try:
            with self.sqlalchemy_engine.connect() as connection:
                for suite_ref in request.suites:
                    metadata = self.suite_registry.resolve(
                        suite_ref, request.execution_engine
                    )
                    payload = json.loads(
                        metadata.definition_path.read_text(encoding="utf-8")
                    )
                    if payload.get("format") != "lakehouse-sql-suite/v1":
                        raise ValidationExecutionError(
                            f"Unsupported SQL suite format in {metadata.definition_path}"
                        )
                    if (
                        metadata.contract_version != request.contract_version
                        or payload.get("contract_version") != request.contract_version
                    ):
                        raise ValidationExecutionError(
                            f"Suite {suite_ref.name}@{suite_ref.version} contract mismatch"
                        )
                    if (
                        payload.get("suite_name") != metadata.name
                        or payload.get("suite_version") != metadata.version
                        or payload.get("effective_from")
                        != metadata.effective_from.isoformat()
                    ):
                        raise ValidationExecutionError(
                            f"Suite identity mismatch in {metadata.definition_path}"
                        )
                    for expectation in payload["expectations"]:
                        evaluated += 1
                        query = expectation["query"].replace(
                            "{data_asset_ref}", request.data_asset_ref
                        )
                        observed = connection.execute(text(query)).scalar_one()
                        result_ids.append(
                            str(
                                uuid5(
                                    NAMESPACE_URL,
                                    f"{request.run_id}|{suite_ref.name}|"
                                    f"{suite_ref.version}|{expectation['id']}",
                                )
                            )
                        )
                        if self._matches(observed, expectation["success"]):
                            successful += 1
                        else:
                            failures.append(
                                ExpectationFailure(
                                    expectation_type=expectation["type"],
                                    severity=expectation["severity"],
                                    column=expectation.get("column"),
                                    observed_value=observed,
                                    message=expectation.get("description"),
                                )
                            )
        except ValidationExecutionError:
            raise
        except Exception as exc:
            raise ValidationExecutionError(
                f"SQL suite execution failed for {request.data_asset_ref}"
            ) from exc
        return EngineValidationResult(
            success=not failures,
            evaluated_expectations=evaluated,
            successful_expectations=successful,
            failures=tuple(failures),
            result_ids=tuple(result_ids),
        )
