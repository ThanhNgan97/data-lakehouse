"""Read-only SQL-suite execution against a request-scoped Nessie branch."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any, Callable
from uuid import NAMESPACE_URL, uuid5

from .errors import ValidationExecutionError
from .models import EngineValidationResult, ExpectationFailure, ValidationRequest
from .suite_registry import SuiteRegistry


_REQUIRED_COLUMN = re.compile(r"(?:VALUES|,)\s*'([A-Za-z_][A-Za-z0-9_]*)'")
_DATE_DIFF_HOUR = re.compile(
    r"date_diff\(\s*'hour'\s*,\s*(.*?)\s*,\s*current_timestamp\s*\)",
    re.IGNORECASE,
)


class CandidateSparkSqlSuiteExecutor:
    """Run reviewed SQL suites through Spark on one immutable Nessie ref."""

    def __init__(
        self,
        suite_registry: SuiteRegistry,
        session_provider: Callable[[str], tuple[Any, str]],
    ) -> None:
        self.suite_registry = suite_registry
        self.session_provider = session_provider

    @staticmethod
    def _matches(observed: Any, success: dict[str, Any]) -> bool:
        expected = success["value"]
        if isinstance(observed, Decimal):
            observed = float(observed)
        operator = success["operator"]
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

    @staticmethod
    def _candidate_query(query: str, request: ValidationRequest, catalog: str) -> str:
        _source_catalog, schema, table = request.data_asset_ref.split(".")
        candidate_asset = f"{catalog}.{schema}.{table}"
        value = query.replace("{data_asset_ref}", candidate_asset)
        value = value.replace("{data_asset_schema}", schema)
        value = value.replace("{data_asset_table}", table)
        alias_match = re.fullmatch(r"(.+)_([0-9a-f]{24})", table)
        if schema == "gold" and alias_match:
            canonical = f"lakehouse.gold.{alias_match.group(1)}"
            value = value.replace(canonical, candidate_asset)
        value = value.replace("lakehouse.silver.", f"{catalog}.silver.")
        value = _DATE_DIFF_HOUR.sub(r"timestampdiff(HOUR, \1, current_timestamp())", value)
        return value.replace("lakehouse.information_schema", f"{catalog}.information_schema")

    @staticmethod
    def _schema_observation(expectation: dict[str, Any], dataframe: Any) -> int:
        required = set(_REQUIRED_COLUMN.findall(expectation["query"]))
        if not required:
            return len(tuple(dataframe.columns))
        return len(required - set(dataframe.columns))

    def __call__(self, request: ValidationRequest) -> EngineValidationResult:
        if not request.nessie_branch or request.nessie_branch == "main":
            raise ValidationExecutionError("Candidate Spark execution requires a non-main branch")
        try:
            spark, catalog = self.session_provider(request.nessie_branch)
            _source_catalog, schema, table = request.data_asset_ref.split(".")
            dataframe = spark.table(f"{catalog}.{schema}.{table}")
            evaluated = successful = 0
            failures: list[ExpectationFailure] = []
            result_ids: list[str] = []
            for suite_ref in request.suites:
                metadata = self.suite_registry.resolve(suite_ref, request.execution_engine)
                payload = json.loads(metadata.definition_path.read_text(encoding="utf-8"))
                if (
                    payload.get("format") != "lakehouse-sql-suite/v1"
                    or payload.get("suite_name") != metadata.name
                    or payload.get("suite_version") != metadata.version
                    or payload.get("contract_version") != request.contract_version
                    or metadata.contract_version != request.contract_version
                ):
                    raise ValidationExecutionError(
                        f"Candidate suite identity mismatch for {suite_ref.name}@{suite_ref.version}"
                    )
                for expectation in payload["expectations"]:
                    evaluated += 1
                    if expectation["type"] == "schema":
                        observed = self._schema_observation(expectation, dataframe)
                    else:
                        query = self._candidate_query(expectation["query"], request, catalog)
                        row = spark.sql(query).first()
                        observed = row[0] if row is not None else None
                    result_ids.append(str(uuid5(
                        NAMESPACE_URL,
                        f"{request.run_id}|{suite_ref.name}|{suite_ref.version}|"
                        f"{expectation['id']}|{request.nessie_commit_hash}",
                    )))
                    if self._matches(observed, expectation["success"]):
                        successful += 1
                    else:
                        failures.append(ExpectationFailure(
                            expectation_type=expectation["type"],
                            severity=expectation["severity"],
                            column=expectation.get("column"),
                            observed_value=observed,
                            message=expectation.get("description"),
                        ))
            return EngineValidationResult(
                success=not failures,
                evaluated_expectations=evaluated,
                successful_expectations=successful,
                failures=tuple(failures),
                result_ids=tuple(result_ids),
            )
        except ValidationExecutionError:
            raise
        except Exception as exc:
            raise ValidationExecutionError(
                f"Candidate Spark SQL validation failed for {request.data_asset_ref}"
            ) from exc
