# -*- coding: utf-8 -*-
"""Unit tests for reusable Silver DQ execution mechanics."""

from __future__ import annotations

import inspect
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

import generic_silver_quality as quality


class GenericSilverQualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day5-generic-silver-quality")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType([
            StructField("value", IntegerType(), True),
            StructField("limit_value", IntegerType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, *rows):
        return self.spark.createDataFrame(list(rows), self.schema)

    def test_add_dq_reasons_preserves_configured_rule_order(self):
        rules = (
            ("VALUE_NULL", "value IS NULL"),
            ("VALUE_NEGATIVE", "value < 0"),
            ("VALUE_EXCEEDS_LIMIT", "value > limit_value"),
        )

        df = self.frame(
            {"value": -1, "limit_value": -2},
        )

        result = quality.add_dq_reasons(df, rules).first()

        self.assertEqual(
            result["_dq_reasons"],
            ["VALUE_NEGATIVE", "VALUE_EXCEEDS_LIMIT"],
        )

    def test_split_dq_routes_valid_and_invalid_rows(self):
        rules = (
            ("VALUE_NULL", "value IS NULL"),
            ("VALUE_NEGATIVE", "value < 0"),
        )

        valid, invalid = quality.split_dq(
            self.frame(
                {"value": 1, "limit_value": 10},
                {"value": -1, "limit_value": 10},
            ),
            rules,
        )

        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 1)
        self.assertEqual(
            invalid.first()["rejection_reason"],
            "VALUE_NEGATIVE",
        )
        self.assertNotIn("_dq_reasons", valid.columns)
        self.assertNotIn("_dq_reasons", invalid.columns)

    def test_split_dq_accumulates_multiple_reasons_with_current_separator(self):
        rules = (
            ("VALUE_NEGATIVE", "value < 0"),
            ("LIMIT_NEGATIVE", "limit_value < 0"),
        )

        _, invalid = quality.split_dq(
            self.frame(
                {"value": -1, "limit_value": -2},
            ),
            rules,
        )

        self.assertEqual(
            invalid.first()["rejection_reason"],
            "VALUE_NEGATIVE|LIMIT_NEGATIVE",
        )

    def test_generic_module_contains_no_learning_outcomes_literals(self):
        source = inspect.getsource(quality)

        forbidden = (
            "ma_chuong_trinh",
            "nam_hoc",
            "hoc_ky",
            "learning_outcomes",
            "learning_outcomes_quarantine",
            "gpa_trung_binh",
            "ty_le_qua_hoc_phan",
        )

        for literal in forbidden:
            self.assertNotIn(literal, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
