from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

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
            suite.meta["contract_version"] = "1.0.0"
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

    def test_phase4_suite_exercises_each_native_spark_expectation(self):
        registry_path = Path("/opt/gx/config/suite_registry.json")
        if not registry_path.exists():
            registry_path = Path("lakehouse/gx/suite_registry.json")
        engine = GreatExpectationsSparkEngine(SuiteRegistry(registry_path))
        request = ValidationRequest(
            data_asset_ref="dataframe://education.teaching_progress/silver",
            dataset_id="education.teaching_progress",
            data_layer="silver",
            execution_engine=ExecutionEngine.SPARK,
            contract_version="1.0.0",
            suites=(SuiteReference("teaching_progress_silver_core", "1.0.0"),),
            run_id="phase4-spark-suite-test",
        )
        schema = StructType(
            [
                StructField("ma_ban_ghi", StringType(), True),
                StructField("ma_don_vi", StringType(), True),
                StructField("ten_don_vi", StringType(), True),
                StructField("ma_lop_hoc_phan", StringType(), True),
                StructField("nam_hoc", StringType(), True),
                StructField("hoc_ky", IntegerType(), True),
                StructField("ty_le_tien_do_giang_day", DoubleType(), True),
                StructField("thoi_gian_cap_nhat_nguon", StringType(), True),
                StructField("_source_system", StringType(), True),
                StructField("_source_type", StringType(), True),
                StructField("_dataset", StringType(), True),
                StructField("_schema_version", StringType(), True),
                StructField("_ingestion_mode", StringType(), True),
                StructField("_batch_id", StringType(), True),
                StructField("_source_updated_at", StringType(), True),
                StructField("_ingested_at", StringType(), True),
                StructField("_record_checksum", StringType(), True),
                StructField("_silver_updated_at", StringType(), True),
            ]
        )
        valid = (
            "record-1", "unit-1", "Unit 1", "class-1", "2025-2026", 1,
            80.0, "2026-01-20T00:00:00Z", "gx", "api", "teaching_progress",
            "1.0", "incremental", "batch-1", "2026-01-20T00:00:00Z",
            "2026-01-20T00:00:00Z", "checksum-1", "2026-01-20T00:00:00Z",
        )

        passing = engine.validate_dataframe(
            self.spark.createDataFrame([valid], schema), request
        )
        self.assertTrue(passing.success)
        self.assertEqual(passing.evaluated_expectations, 11)

        missing_column = engine.validate_dataframe(
            self.spark.createDataFrame([valid], schema).drop("ten_don_vi"), request
        )
        self.assertTrue(
            any(f.expectation_type == "expect_table_columns_to_match_set" for f in missing_column.failures)
        )

        null_row = list(valid)
        for index in (0, 1, 3, 4, 5, 6, 14):
            null_row[index] = None
        nulls = engine.validate_dataframe(
            self.spark.createDataFrame([tuple(null_row)], schema), request
        )
        self.assertEqual(
            sum(f.expectation_type == "expect_column_values_to_not_be_null" for f in nulls.failures),
            7,
        )

        duplicate = engine.validate_dataframe(
            self.spark.createDataFrame([valid, valid], schema), request
        )
        self.assertTrue(
            any(f.expectation_type == "expect_compound_columns_to_be_unique" and f.severity == "warning" for f in duplicate.failures)
        )

        empty = engine.validate_dataframe(
            self.spark.createDataFrame([], schema), request
        )
        self.assertTrue(
            any(f.expectation_type == "expect_table_row_count_to_be_between" for f in empty.failures)
        )

        out_of_range = list(valid)
        out_of_range[6] = 101.0
        ranged = engine.validate_dataframe(
            self.spark.createDataFrame([tuple(out_of_range)], schema), request
        )
        self.assertTrue(
            any(f.expectation_type == "expect_column_values_to_be_between" for f in ranged.failures)
        )


if __name__ == "__main__":
    unittest.main()
