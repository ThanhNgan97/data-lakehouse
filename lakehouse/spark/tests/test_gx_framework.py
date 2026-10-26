"""Tests for the reusable persistent GX framework."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import great_expectations as gx
from pyspark.sql import SparkSession

from data_quality.exceptions import (
    GXProjectConfigurationError,
    GXSuiteNotFoundError,
)
from data_quality.gx_runner import load_file_context, validate_dataframe
from data_quality.suite_registry import SuiteRegistry
from data_quality.validation_policy import ValidationPolicy


class GXFrameworkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("gx-framework-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        project_root = Path(self.temp_dir.name)
        context = gx.get_context(
            mode="file",
            project_root_dir=str(project_root),
        )
        self.context_root = Path(context.root_directory)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _add_suite(self, name: str, *, severity: str = "critical") -> None:
        context = load_file_context(self.context_root)
        suite = gx.ExpectationSuite(name=name)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="progress",
                min_value=0,
                max_value=100,
                severity=severity,
            )
        )
        context.suites.add(suite)

    def test_file_context_is_loaded_from_explicit_root(self):
        context = load_file_context(self.context_root)

        self.assertEqual(
            Path(context.root_directory).resolve(),
            self.context_root.resolve(),
        )

    def test_missing_file_context_fails_without_ephemeral_fallback(self):
        missing_root = Path(self.temp_dir.name) / "missing"

        with self.assertRaises(GXProjectConfigurationError):
            load_file_context(missing_root)

    def test_missing_suite_fails_before_validation(self):
        dataframe = self.spark.createDataFrame([{"progress": 80.0}])

        with self.assertRaises(GXSuiteNotFoundError):
            validate_dataframe(
                dataframe,
                suite_names=["missing_suite"],
                dataset_id="education.teaching_progress",
                data_layer="silver",
                run_id="missing-suite-run",
                context_root=self.context_root,
            )

    def test_critical_failure_blocks_validation(self):
        self._add_suite("critical_progress")
        dataframe = self.spark.createDataFrame([{"progress": 101.0}])

        summary = validate_dataframe(
            dataframe,
            suite_names=["critical_progress"],
            dataset_id="education.teaching_progress",
            data_layer="silver",
            run_id="critical-run",
            table_name="lakehouse.silver.teaching_progress",
            nessie_branch="candidate_critical",
            context_root=self.context_root,
        )

        self.assertFalse(summary.success)
        self.assertFalse(summary.raw_gx_success)
        self.assertEqual(summary.evaluated_expectations, 1)
        self.assertEqual(summary.failed_expectations, 1)
        self.assertEqual(len(summary.critical_failures), 1)
        self.assertEqual(len(summary.warning_failures), 0)
        self.assertEqual(summary.critical_failures[0].column, "progress")
        self.assertEqual(summary.nessie_branch, "candidate_critical")

    def test_warning_failure_does_not_block_policy_success(self):
        self._add_suite("warning_progress", severity="warning")
        dataframe = self.spark.createDataFrame([{"progress": 101.0}])

        summary = validate_dataframe(
            dataframe,
            suite_names=["warning_progress"],
            dataset_id="education.teaching_progress",
            data_layer="silver",
            run_id="warning-run",
            context_root=self.context_root,
        )

        self.assertTrue(summary.success)
        self.assertFalse(summary.raw_gx_success)
        self.assertEqual(len(summary.critical_failures), 0)
        self.assertEqual(len(summary.warning_failures), 1)

    def test_repeated_validation_reuses_persistent_resources(self):
        self._add_suite("repeatable_progress")
        dataframe = self.spark.createDataFrame([{"progress": 80.0}])

        first = validate_dataframe(
            dataframe,
            suite_names=["repeatable_progress"],
            dataset_id="education.teaching.progress",
            data_layer="silver",
            run_id="repeat-one",
            context_root=self.context_root,
        )
        second = validate_dataframe(
            dataframe,
            suite_names=["repeatable_progress"],
            dataset_id="education.teaching.progress",
            data_layer="silver",
            run_id="repeat-two",
            context_root=self.context_root,
        )

        self.assertTrue(first.success)
        self.assertTrue(second.success)
        self.assertEqual(first.evaluated_expectations, 1)
        self.assertEqual(second.evaluated_expectations, 1)

    def test_summary_is_json_compatible_primitives(self):
        self._add_suite("serializable_progress")
        dataframe = self.spark.createDataFrame([{"progress": 80.0}])

        summary = validate_dataframe(
            dataframe,
            suite_names=["serializable_progress"],
            dataset_id="education.teaching_progress",
            data_layer="silver",
            run_id="serialize-run",
            context_root=self.context_root,
        )

        payload = summary.as_dict()
        self.assertEqual(payload["suite_names"], ["serializable_progress"])
        self.assertEqual(payload["critical_failures"], [])
        self.assertIsInstance(payload["validation_result_ids"], list)


class ValidationPolicyTest(unittest.TestCase):
    def test_default_policy_blocks_only_critical(self):
        policy = ValidationPolicy()

        self.assertTrue(policy.is_blocking("CRITICAL"))
        self.assertFalse(policy.is_blocking("warning"))
        self.assertTrue(policy.is_blocking(None))


class SuiteRegistryTest(unittest.TestCase):
    def test_registration_is_dataset_and_layer_specific(self):
        registry = SuiteRegistry()
        registry.register(
            "education.teaching_progress",
            "SILVER",
            ["silver_common", "silver_teaching_progress"],
        )

        self.assertEqual(
            registry.resolve("education.teaching_progress", "silver"),
            ("silver_common", "silver_teaching_progress"),
        )
        self.assertFalse(
            registry.is_registered("education.teaching_progress", "gold")
        )

    def test_registry_rejects_an_empty_suite_list(self):
        registry = SuiteRegistry()

        with self.assertRaises(ValueError):
            registry.register("education.teaching_progress", "silver", [])


if __name__ == "__main__":
    unittest.main()
