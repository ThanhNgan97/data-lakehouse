"""Runtime compatibility smoke tests for GX Core and Spark.

These tests intentionally use an ephemeral GX context. Phase 1 only proves
that the pinned runtime can validate Spark DataFrames; persistent project
configuration belongs to the next phase.
"""

from __future__ import annotations

import unittest
import uuid

import great_expectations as gx
from pyspark.sql import SparkSession


class GXRuntimeSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("gx-runtime-smoke")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _validate(self, rows):
        suffix = uuid.uuid4().hex
        dataframe = self.spark.createDataFrame(rows)
        context = gx.get_context(mode="ephemeral")

        data_source = context.data_sources.add_spark(
            name=f"spark_{suffix}",
        )
        asset = data_source.add_dataframe_asset(
            name=f"asset_{suffix}",
        )
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name=f"batch_{suffix}",
        )

        suite = gx.ExpectationSuite(name=f"suite_{suffix}")
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="id",
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="progress",
                min_value=0,
                max_value=100,
            )
        )
        context.suites.add(suite)

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"validation_{suffix}",
                data=batch_definition,
                suite=suite,
            )
        )
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"checkpoint_{suffix}",
                validation_definitions=[validation_definition],
            )
        )

        return checkpoint.run(
            batch_parameters={"dataframe": dataframe},
        )

    def test_valid_spark_dataframe_passes(self):
        result = self._validate(
            [
                {"id": 1, "progress": 80.0},
                {"id": 2, "progress": 100.0},
            ]
        )

        self.assertTrue(result.success)

    def test_invalid_spark_dataframe_fails(self):
        result = self._validate(
            [
                {"id": 1, "progress": 101.0},
                {"id": 2, "progress": 80.0},
            ]
        )

        self.assertFalse(result.success)


if __name__ == "__main__":
    unittest.main()
