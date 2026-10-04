# -*- coding: utf-8 -*-
"""Unit tests for reusable Silver quarantine routing mechanics."""

from __future__ import annotations

import inspect
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import generic_silver_quarantine as quarantine


class GenericSilverQuarantineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day5-generic-silver-quarantine")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType([
            StructField("key_a", StringType(), True),
            StructField("key_b", StringType(), True),
            StructField("key_c", IntegerType(), True),
            StructField("payload", StringType(), True),
            StructField("rejection_reason", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, *rows):
        return self.spark.createDataFrame(list(rows), self.schema)

    def test_business_key_text_preserves_configured_order_and_null_sentinel(self):
        df = self.frame({
            "key_a": "A",
            "key_b": None,
            "key_c": 2,
            "payload": "x",
            "rejection_reason": "RULE_1",
        })

        row = quarantine.add_quarantine_metadata(
            df,
            ("key_a", "key_b", "key_c"),
        ).first()

        self.assertEqual(
            row["_business_key"],
            "A|<NULL>|2",
        )
        self.assertIsNotNone(row["rejected_at"])

    def test_prepare_quarantine_rows_preserves_exact_output_column_order(self):
        df = self.frame({
            "key_a": "A",
            "key_b": "B",
            "key_c": 1,
            "payload": "x",
            "rejection_reason": "RULE_1",
        })

        output_columns = (
            "key_a",
            "payload",
            "_business_key",
            "rejection_reason",
            "rejected_at",
        )

        prepared = quarantine.prepare_quarantine_rows(
            df,
            business_key=("key_a", "key_b", "key_c"),
            output_columns=output_columns,
        )

        self.assertEqual(
            tuple(prepared.columns),
            output_columns,
        )
        row = prepared.first()
        self.assertEqual(row["_business_key"], "A|B|1")
        self.assertEqual(row["rejection_reason"], "RULE_1")

    def test_empty_quarantine_write_returns_zero_without_touching_target(self):
        empty = self.spark.createDataFrame([], self.schema)

        count = quarantine.write_quarantine_rows(
            empty,
            target_table="this_table_must_not_be_touched",
            business_key=("key_a", "key_b", "key_c"),
            output_columns=(
                "key_a",
                "payload",
                "_business_key",
                "rejection_reason",
                "rejected_at",
            ),
        )

        self.assertEqual(count, 0)

    def test_generic_module_contains_no_learning_outcomes_literals(self):
        source = inspect.getsource(quarantine)

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
