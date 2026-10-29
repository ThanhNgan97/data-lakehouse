from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from gx_runtime.engines import VersionedSqlSuiteExecutor
from gx_runtime.errors import ValidationConfigurationError, ValidationExecutionError
from gx_runtime.models import ExecutionEngine, SuiteReference, ValidationRequest
from gx_runtime.suite_registry import SuiteRegistry


GX_ROOT = Path("/opt/gx/config")
if not GX_ROOT.exists():
    GX_ROOT = Path(__file__).resolve().parents[2] / "gx"
REGISTRY = SuiteRegistry(GX_ROOT / "suite_registry.json")


def sql_request(name: str, asset: str) -> ValidationRequest:
    return ValidationRequest(
        data_asset_ref=asset,
        dataset_id="education.teaching_progress",
        data_layer="silver" if "silver_core" in name else "gold",
        execution_engine=ExecutionEngine.SQL,
        contract_version="1.0.0",
        suites=(SuiteReference(name, "1.0.0"),),
        run_id=f"phase4-{name}",
        nessie_branch="main",
        snapshot_id="fixture-snapshot",
        nessie_commit_hash="fixture-commit",
    )


def passing_value(success):
    operator = success["operator"]
    expected = success["value"]
    if operator in {"eq", "lte", "gte"}:
        return expected
    if operator == "between":
        return expected[0]
    raise AssertionError(operator)


def failing_value(success):
    operator = success["operator"]
    expected = success["value"]
    if operator == "eq":
        return expected + 1
    if operator == "lte":
        return expected + 1
    if operator == "gte":
        return expected - 1
    if operator == "between":
        return expected[1] + 1
    raise AssertionError(operator)


class FakeResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class FakeConnection:
    def __init__(self, values):
        self.values = iter(values)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _query):
        return FakeResult(next(self.values))


class Phase4SuiteRegistryTest(unittest.TestCase):
    def test_metadata_and_engine_specific_definitions_are_versioned(self):
        silver = SuiteReference("teaching_progress_silver_core", "1.0.0")
        spark = REGISTRY.resolve(silver, ExecutionEngine.SPARK)
        sql = REGISTRY.resolve(silver, ExecutionEngine.SQL)

        self.assertEqual(spark.contract_version, "1.0.0")
        self.assertEqual(spark.effective_from.isoformat(), "2026-10-27")
        self.assertTrue(spark.definition_path.name.endswith("spark.json"))
        self.assertTrue(sql.definition_path.name.endswith("sql.json"))
        self.assertNotEqual(spark.definition_path, sql.definition_path)

    def test_gold_and_reconciliation_are_sql_only(self):
        for name in (
            "teaching_progress_gold_core",
            "teaching_progress_silver_gold_reconciliation",
        ):
            with self.subTest(name=name):
                REGISTRY.resolve(SuiteReference(name, "1.0.0"), ExecutionEngine.SQL)
                with self.assertRaises(ValidationConfigurationError):
                    REGISTRY.resolve(
                        SuiteReference(name, "1.0.0"), ExecutionEngine.SPARK
                    )


class VersionedSqlSuiteExecutorTest(unittest.TestCase):
    SUITES = (
        ("teaching_progress_silver_core", "lakehouse.silver.teaching_progress"),
        ("teaching_progress_gold_core", "lakehouse.gold.teaching_progress_metrics"),
        (
            "teaching_progress_silver_gold_reconciliation",
            "lakehouse.gold.teaching_progress_metrics",
        ),
        ("learning_outcomes_silver_core", "lakehouse.silver.learning_outcomes"),
        (
            "learning_outcomes_gold_core",
            "lakehouse.gold.learning_outcomes_metrics",
        ),
        (
            "learning_outcomes_silver_gold_reconciliation",
            "lakehouse.gold.learning_outcomes_metrics",
        ),
    )

    def _payload(self, name):
        metadata = REGISTRY.resolve(
            SuiteReference(name, "1.0.0"), ExecutionEngine.SQL
        )
        return json.loads(metadata.definition_path.read_text(encoding="utf-8"))

    def test_every_sql_expectation_passes_and_fails_independently(self):
        for name, asset in self.SUITES:
            payload = self._payload(name)
            expectations = payload["expectations"]
            for failed_index, expected_failure in enumerate(expectations):
                with self.subTest(suite=name, expectation=expected_failure["id"]):
                    values = [passing_value(item["success"]) for item in expectations]
                    values[failed_index] = failing_value(expected_failure["success"])
                    sqlalchemy_engine = MagicMock()
                    sqlalchemy_engine.connect.return_value = FakeConnection(values)
                    result = VersionedSqlSuiteExecutor(
                        REGISTRY, sqlalchemy_engine
                    )(sql_request(name, asset))

                    self.assertEqual(result.evaluated_expectations, len(expectations))
                    self.assertEqual(result.successful_expectations, len(expectations) - 1)
                    self.assertFalse(result.success)
                    self.assertEqual(len(result.failures), 1)
                    self.assertEqual(
                        result.failures[0].expectation_type,
                        expected_failure["type"],
                    )
                    self.assertEqual(
                        result.failures[0].severity,
                        expected_failure["severity"],
                    )

    def test_every_sql_expectation_passes_as_a_suite(self):
        for name, asset in self.SUITES:
            with self.subTest(suite=name):
                expectations = self._payload(name)["expectations"]
                values = [passing_value(item["success"]) for item in expectations]
                sqlalchemy_engine = MagicMock()
                sqlalchemy_engine.connect.return_value = FakeConnection(values)
                result = VersionedSqlSuiteExecutor(
                    REGISTRY, sqlalchemy_engine
                )(sql_request(name, asset))
                self.assertTrue(result.success)
                self.assertEqual(result.evaluated_expectations, len(expectations))

    def test_shared_baseline_suite_runs_for_an_unregistered_silver_asset(self):
        request = ValidationRequest(
            data_asset_ref="lakehouse.silver.dynamic_students",
            dataset_id="dynamic.dynamic_students",
            data_layer="silver",
            execution_engine=ExecutionEngine.SQL,
            contract_version="baseline-1.0.0",
            suites=(SuiteReference("lakehouse_baseline_silver", "1.0.0"),),
            run_id="baseline-dynamic",
            nessie_branch="main",
            snapshot_id="7",
            nessie_commit_hash="commit-7",
        )
        sqlalchemy_engine = MagicMock()
        sqlalchemy_engine.connect.return_value = FakeConnection([1, 10, 0])

        result = VersionedSqlSuiteExecutor(REGISTRY, sqlalchemy_engine)(request)

        self.assertTrue(result.success)
        self.assertEqual(result.evaluated_expectations, 3)

    def test_unsafe_asset_identifier_is_rejected_before_query_execution(self):
        request = sql_request(
            "teaching_progress_silver_core",
            "lakehouse.silver.teaching_progress; DROP TABLE x",
        )
        with self.assertRaisesRegex(ValidationExecutionError, "Unsafe"):
            VersionedSqlSuiteExecutor(REGISTRY, MagicMock())(request)


if __name__ == "__main__":
    unittest.main()
