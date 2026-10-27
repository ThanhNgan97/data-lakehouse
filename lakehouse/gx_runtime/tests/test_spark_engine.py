from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import great_expectations as gx
from pyspark.sql import SparkSession

from gx_runtime.engines import GreatExpectationsSparkEngine
from gx_runtime.models import ExecutionEngine, SuiteReference, ValidationRequest
from gx_runtime.suite_registry import SuiteRegistry


class GreatExpectationsSparkEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("gx-framework-engine-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_engine_executes_git_versioned_suite_without_process_state(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            gx.get_context(mode="ephemeral")
            suite = gx.ExpectationSuite(name="progress_range")
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="progress", min_value=0, max_value=100
                )
            )
            (root / "progress_range.json").write_text(
                json.dumps(suite.to_json_dict()), encoding="utf-8"
            )
            (root / "registry.json").write_text(
                json.dumps(
                    {
                        "suites": [
                            {
                                "name": "progress_range",
                                "version": "1.0.0",
                                "supported_engines": ["spark"],
                                "contract_version": "1.0.0",
                                "effective_from": "2026-10-27",
                                "definition": "progress_range.json",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            engine = GreatExpectationsSparkEngine(SuiteRegistry(root / "registry.json"))
            request = ValidationRequest(
                data_asset_ref="dataframe://education.teaching_progress/silver",
                dataset_id="education.teaching_progress",
                data_layer="silver",
                execution_engine=ExecutionEngine.SPARK,
                contract_version="1.0.0",
                suites=(SuiteReference("progress_range", "1.0.0"),),
                run_id="spark-engine-test",
            )

            passing = engine.validate_dataframe(
                self.spark.createDataFrame([{"progress": 80.0}]), request
            )
            failing = engine.validate_dataframe(
                self.spark.createDataFrame([{"progress": 101.0}]), request
            )

        self.assertTrue(passing.success)
        self.assertFalse(failing.success)
        self.assertEqual(len(failing.failures), 1)


if __name__ == "__main__":
    unittest.main()
